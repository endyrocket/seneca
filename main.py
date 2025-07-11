#!/usr/bin/env python3

import argparse
import logging
import sys
import time
import json
import os
import subprocess
import uuid
import queue
import threading
from datetime import datetime, timezone, timedelta
import hashlib

# Flask for webhook listener
from flask import Flask, request, jsonify

# Prometheus client for exposing metrics
from prometheus_client import make_wsgi_app, REGISTRY as prometheus_global_registry
from werkzeug.middleware.dispatcher import DispatcherMiddleware


# Prometheus API client for querying Prometheus
from prometheus_api_client import PrometheusConnect, PrometheusApiClientException

# Kubernetes client
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

# OpenTelemetry imports
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
# We will use the Prometheus exporter for OpenTelemetry
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

# Requests for sending to Slack
import requests

# OpenAI client
import openai

# --- Configuration ---
SERVICE_NAME_OTEL = "Seneca-RCA-Service"
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
DEBUG_POD_NAMESPACE = os.environ.get('DEBUG_POD_NAMESPACE', 'default')
PROMETHEUS_URL_ENV = os.environ.get('PROMETHEUS_URL') # For querying workloads for context

# OpenAI Configuration
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', None)
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', "o3")

# Time window duration (in minutes) to look back from the alert's start time
# for running GPU pods.
WORKLOAD_QUERY_WINDOW_MINUTES = 30

# --- Queue Configuration ---
MAX_QUEUE_SIZE = 100  # Maximum number of alerts in queue
MAX_WORKER_THREADS = 1  # Number of concurrent RCA processing threads (reduced to avoid K8s API conflicts)
QUEUE_TIMEOUT_SECONDS = 30  # Timeout for queue operations

# --- RCA Deduplication Configuration ---
RCA_CACHE_TTL_HOURS = 24  # How long to remember processed RCAs (in hours)
RCA_CACHE_CLEANUP_INTERVAL_MINUTES = 60  # How often to clean up expired entries (in minutes)

# --- Constants for Log Fetching Pod ---
LOG_POD_NAMESPACE = os.getenv("DEBUG_POD_NAMESPACE", "default")
LOG_POD_IMAGE = os.getenv("LOG_FETCH_IMAGE", "busybox:latest") # busybox is small and has tail, cat
LOG_POD_PULL_POLICY = "IfNotPresent"
LOG_POD_CREATION_TIMEOUT_SECONDS = 120 # Timeout for pod to become ready/failed
LOG_POD_EXEC_TIMEOUT_SECONDS = 60 # Timeout for the exec command itself
LOG_POD_DEFAULT_TAIL_LINES = "200" # Number of lines to tail by default

# --- Log Fetching Configuration ---
SKIP_LOG_FETCHING = os.environ.get('SKIP_LOG_FETCHING', 'false').lower() == 'true'  # Skip log fetching if K8s has issues

# --- Auto-Cordoning Configuration ---
ENABLE_AUTO_CORDON = os.environ.get('ENABLE_AUTO_CORDON', 'true').lower() == 'true'  # Enable automatic node cordoning
AUTO_CORDON_CONFIDENCE_THRESHOLD = int(os.environ.get('AUTO_CORDON_CONFIDENCE_THRESHOLD', '80'))  # Confidence threshold for auto-cordoning

# --- DCGM Metrics Configuration ---
DCGM_METRICS_WINDOW_MINUTES = 30  # Time window to look back for DCGM metrics
DCGM_ANOMALY_THRESHOLDS = {
    'gpu_temp_high': 85,  # Celsius - high temperature threshold
    'gpu_temp_critical': 95,  # Celsius - critical temperature threshold
    'mem_util_high': 90,  # Percentage - high memory utilization
    'pcie_traffic_high': 15,  # GB/s - high PCIe traffic threshold
    'thermal_violation_count': 1,  # Number of thermal violations to flag
    'power_violation_count': 1,   # Number of power violations to flag
    'nvlink_flit_errors_high': 100,  # Number of NVLink flit errors to flag as high
    'nvlink_flit_errors_critical': 1000,  # Number of NVLink flit errors to flag as critical
    'ecc_sbe_errors_high': 10,  # Number of single-bit ECC errors to flag as high
    'ecc_sbe_errors_critical': 100,  # Number of single-bit ECC errors to flag as critical
    'ecc_dbe_errors_count': 1  # Any double-bit ECC errors are critical
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('seneca-rca-service')

# --- Custom Exception ---
class RcaServiceError(Exception):
    """Base exception for this service."""
    pass

# --- OpenTelemetry Metrics Setup (Exposing via Prometheus) ---
otel_metrics_instruments = {} # Global dict for OTel instruments

def setup_otel_metrics_for_prometheus(service_name=SERVICE_NAME_OTEL):
    """
    Initialise the OpenTelemetry SDK with a Prometheus *reader*.
    The reader registers its collector with the global prometheus_client.core.REGISTRY.
    This function will return that global registry.
    """
    global otel_metrics_instruments
    logger.info(
        "Setting up OpenTelemetry metrics for Prometheus exposition "
        f"for service: {service_name}"
    )

    prom_reader = PrometheusMetricReader()

    resource = Resource(attributes={SERVICE_NAME: service_name})

    provider = MeterProvider(resource=resource, metric_readers=[prom_reader])
    metrics.set_meter_provider(provider)

    meter = metrics.get_meter("sre.xid.rca.meter.prometheus")

    otel_metrics_instruments["meter"] = meter
    otel_metrics_instruments["alerts_received"] = meter.create_counter(
        name="rca_alerts_received_total",
        description="Total number of XID alerts received.",
        unit="1",
    )
    otel_metrics_instruments["successful_rca"] = meter.create_counter(
        name="rca_successful_total",
        description="Total number of XID RCAs successfully processed.",
        unit="1",
    )
    otel_metrics_instruments["failed_rca"] = meter.create_counter(
        name="rca_failed_total",
        description="Total number of XID RCAs that failed.",
        unit="1",
    )
    otel_metrics_instruments["log_fetch_duration"] = meter.create_histogram(
        name="rca_log_fetch_duration_seconds",
        description="Duration of log fetching.",
        unit="s",
    )
    otel_metrics_instruments["workload_query_duration"] = meter.create_histogram(
        name="rca_workload_query_duration_seconds",
        description="Duration of Prometheus workload query.",
        unit="s",
    )
    otel_metrics_instruments["llm_call_duration"] = meter.create_histogram(
        name="rca_llm_call_duration_seconds",
        description="Duration of LLM API call.",
        unit="s",
    )

    # Queue metrics
    otel_metrics_instruments["queue_size"] = meter.create_up_down_counter(
        name="rca_queue_size",
        description="Current number of alerts in the processing queue.",
        unit="1",
    )
    otel_metrics_instruments["queue_full_drops"] = meter.create_counter(
        name="rca_queue_full_drops_total",
        description="Total number of alerts dropped due to full queue.",
        unit="1",
    )
    otel_metrics_instruments["processing_duration"] = meter.create_histogram(
        name="rca_processing_duration_seconds",
        description="Total duration of RCA processing per alert.",
        unit="s",
    )

    # Cache metrics
    otel_metrics_instruments["cache_hits"] = meter.create_counter(
        name="rca_cache_hits_total",
        description="Total number of RCA cache hits (duplicate alerts avoided).",
        unit="1",
    )
    otel_metrics_instruments["cache_misses"] = meter.create_counter(
        name="rca_cache_misses_total",
        description="Total number of RCA cache misses (new RCAs processed).",
        unit="1",
    )
    otel_metrics_instruments["cache_size"] = meter.create_up_down_counter(
        name="rca_cache_size",
        description="Current number of entries in the RCA cache.",
        unit="1",
    )

    # DCGM metrics
    otel_metrics_instruments["dcgm_collection_duration"] = meter.create_histogram(
        name="rca_dcgm_collection_duration_seconds",
        description="Duration of DCGM metrics collection.",
        unit="s",
    )
    otel_metrics_instruments["dcgm_anomalies_detected"] = meter.create_counter(
        name="rca_dcgm_anomalies_detected_total",
        description="Total number of DCGM anomalies detected.",
        unit="1",
    )
    otel_metrics_instruments["dcgm_metrics_collected"] = meter.create_counter(
        name="rca_dcgm_metrics_collected_total",
        description="Total number of DCGM metrics successfully collected.",
        unit="1",
    )

    logger.info("OpenTelemetry PrometheusMetricReader initialized. Returning global Prometheus registry.")
    return prometheus_global_registry


# --- Kubernetes & Prometheus Client Setup ---
k8s_core_v1_api = None
prom_api_client = None 
openai_client = None

# --- Queue and Worker Thread Management ---
alert_queue = None
worker_threads = []
shutdown_event = threading.Event()

# --- RCA Deduplication Cache ---
rca_cache = {}  # Dictionary to store processed RCAs: {cache_key: {"timestamp": datetime, "result": rca_result}}
rca_cache_lock = threading.Lock()  # Thread-safe access to cache
cache_cleanup_thread = None

# --- Log Fetching Lock ---
log_fetch_lock = threading.Lock()  # Prevent concurrent log fetching operations

def initialize_clients(prometheus_url_arg=None):
    global k8s_core_v1_api, prom_api_client, openai_client
    # Kubernetes Client
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes configuration.")
    except config.ConfigException:
        try:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes configuration from kubeconfig.")
        except config.ConfigException as e:
            logger.error(f"Could not configure Kubernetes client: {e}")
            raise RcaServiceError(f"Kubernetes client initialization failed: {e}")
    k8s_core_v1_api = client.CoreV1Api()

    # Prometheus API Client (for querying context)
    effective_prometheus_url = prometheus_url_arg or PROMETHEUS_URL_ENV
    if effective_prometheus_url:
        try:
            disable_ssl_prom = effective_prometheus_url.startswith('http://')
            prom_api_client = PrometheusConnect(url=effective_prometheus_url, disable_ssl=disable_ssl_prom)
            prom_api_client.check_prometheus_connection()
            logger.info(f"Successfully connected to Prometheus (for context query) at {effective_prometheus_url}")
        except PrometheusApiClientException as e:
            logger.error(f"Failed to connect to Prometheus (for context query) at {effective_prometheus_url}: {e}")
            prom_api_client = None
    else:
        logger.warning("Prometheus URL for context query not configured. Workload identification will be skipped.")

    # OpenAI Client
    if not OPENAI_API_KEY:
        logger.warning("OpenAI API key is not configured. RCA processing will fail without valid API key.")
        openai_client = None
    else:
        try:
            openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
            logger.info(f"OpenAI client initialized for model: {OPENAI_MODEL}")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            openai_client = None


# --- Queue Management Functions ---
def initialize_queue():
    """Initialize the alert processing queue and start worker threads."""
    global alert_queue, worker_threads, cache_cleanup_thread
    
    logger.info(f"Initializing alert queue with max size: {MAX_QUEUE_SIZE}")
    alert_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
    
    # Start worker threads
    logger.info(f"Starting {MAX_WORKER_THREADS} worker threads")
    for i in range(MAX_WORKER_THREADS):
        thread = threading.Thread(
            target=queue_worker,
            name=f"RCA-Worker-{i+1}",
            daemon=True
        )
        thread.start()
        worker_threads.append(thread)
        logger.info(f"Started worker thread: {thread.name}")
    
    # Start cache cleanup thread
    logger.info("Starting RCA cache cleanup thread")
    cache_cleanup_thread = threading.Thread(
        target=cache_cleanup_worker,
        name="RCA-Cache-Cleanup",
        daemon=True
    )
    cache_cleanup_thread.start()
    logger.info(f"Started cache cleanup thread: {cache_cleanup_thread.name}")

def queue_worker():
    """Worker thread function that processes alerts from the queue."""
    thread_name = threading.current_thread().name
    logger.info(f"Worker thread {thread_name} started")
    
    while not shutdown_event.is_set():
        try:
            # Get alert from queue with timeout
            alert_item = alert_queue.get(timeout=1.0)
            
            if alert_item is None:  # Shutdown signal
                break
                
            logger.info(f"Worker {thread_name} processing alert: {alert_item.get('fingerprint', 'unknown')}")
            
            # Process the alert
            try:
                process_alert_rca(alert_item)
                logger.info(f"Worker {thread_name} successfully processed alert")
                
                # Update metrics
                if "successful_rca" in otel_metrics_instruments:
                    otel_metrics_instruments["successful_rca"].add(1)
                    
            except Exception as e:
                logger.error(f"Worker {thread_name} failed to process alert: {e}")
                
                # Update metrics
                if "failed_rca" in otel_metrics_instruments:
                    otel_metrics_instruments["failed_rca"].add(1)
                    
            finally:
                alert_queue.task_done()
                
        except queue.Empty:
            # Timeout occurred, continue loop
            continue
        except Exception as e:
            logger.error(f"Worker {thread_name} encountered unexpected error: {e}")
    
    logger.info(f"Worker thread {thread_name} shutting down")

def enqueue_alert(alert_data):
    """Add an alert to the processing queue."""
    try:
        alert_queue.put(alert_data, timeout=QUEUE_TIMEOUT_SECONDS)
        logger.info(f"Alert queued for processing: {alert_data.get('fingerprint', 'unknown')}")
        
        # Update queue size metric
        if "queue_size" in otel_metrics_instruments:
            otel_metrics_instruments["queue_size"].add(1)
            
        return True
    except queue.Full:
        logger.error("Alert queue is full, dropping alert")
        
        # Update queue full drops metric
        if "queue_full_drops" in otel_metrics_instruments:
            otel_metrics_instruments["queue_full_drops"].add(1)
            
        return False
    except Exception as e:
        logger.error(f"Failed to enqueue alert: {e}")
        return False

def shutdown_queue():
    """Gracefully shutdown the queue and worker threads."""
    global worker_threads, cache_cleanup_thread
    
    logger.info("Shutting down queue and worker threads...")
    shutdown_event.set()
    
    # Send shutdown signals to workers
    for _ in worker_threads:
        try:
            alert_queue.put(None, timeout=1.0)
        except queue.Full:
            pass
    
    # Wait for workers to finish
    for thread in worker_threads:
        thread.join(timeout=5.0)
        if thread.is_alive():
            logger.warning(f"Worker thread {thread.name} did not shutdown gracefully")
    
    # Wait for cache cleanup thread to finish
    if cache_cleanup_thread and cache_cleanup_thread.is_alive():
        cache_cleanup_thread.join(timeout=5.0)
        if cache_cleanup_thread.is_alive():
            logger.warning("Cache cleanup thread did not shutdown gracefully")
    
    logger.info("Queue shutdown complete")

def process_alert_rca(alert_data):
    """Process a single alert through the full RCA pipeline."""
    start_time = time.time()
    
    try:
        # Extract alert information
        node_name = alert_data.get('labels', {}).get('node', 'unknown')
        logger.info(f"Starting RCA for alert on node: {node_name}")
        
        # Parse the alert data first
        parsed_alert = parse_alertmanager_payload([alert_data])  # Wrap in list for consistency
        
        # Generate cache key for deduplication
        cache_key = generate_rca_cache_key(
            parsed_alert['node_name'],
            parsed_alert['xid_error_code'],
            parsed_alert['alert_name']
        )
        
        # Check if we've already processed this RCA recently
        is_cached, cached_result = is_rca_already_processed(cache_key)
        if is_cached:
            logger.info(f"Using cached RCA result for node {parsed_alert['node_name']}, "
                       f"XID {parsed_alert['xid_error_code']}, alert {parsed_alert['alert_name']}")
            
            # Send cached result to Slack with a note that it's cached
            if SLACK_WEBHOOK_URL:
                # Add a note to the cached result indicating it's from cache
                cached_result_with_note = cached_result.copy()
                if 'Summary' in cached_result_with_note:
                    cached_result_with_note['Summary'] = f"[CACHED] {cached_result_with_note['Summary']}"
                
                slack_message = format_slack_message(parsed_alert, cached_result_with_note, [], {}, {}, {})
                send_to_slack(slack_message, SLACK_WEBHOOK_URL)
            
            processing_time = time.time() - start_time
            logger.info(f"Cached RCA processing completed in {processing_time:.2f} seconds")
            
            # Record processing duration metric
            if "processing_duration" in otel_metrics_instruments:
                otel_metrics_instruments["processing_duration"].record(processing_time, {
                    "node_name": node_name,
                    "cached": "true"
                })
            
            return  # Exit early for cached results
        
        # Proceed with full RCA processing for new alerts
        logger.info(f"Processing new RCA for node {parsed_alert['node_name']}, "
                   f"XID {parsed_alert['xid_error_code']}, alert {parsed_alert['alert_name']}")
        
        # Fetch logs (skip if configured to do so)
        logs = {}
        log_files = [
            ("kern_log", "/var/log/kern.log"),
            ("dmesg_log", "/var/log/dmesg"), 
            ("syslog", "/var/log/syslog")
        ]
        
        if SKIP_LOG_FETCHING:
            logger.warning("Log fetching is disabled (SKIP_LOG_FETCHING=true). RCA will be based on alert data and workload context only.")
            for log_name, _ in log_files:
                logs[log_name] = "Log fetching disabled due to Kubernetes connectivity issues."
        else:
            with log_fetch_lock:  # Serialize log fetching operations across worker threads
                for log_name, log_path in log_files:
                    try:
                        logs[log_name] = fetch_node_log_sdk(parsed_alert['node_name'], log_path)
                    except Exception as e:
                        logger.error(f"Failed to fetch {log_name}: {e}")
                        logs[log_name] = f"Failed to fetch {log_name}: {str(e)}"
        
        # Extract individual logs for backward compatibility
        kern_log = logs.get("kern_log", "")
        dmesg_log = logs.get("dmesg_log", "")
        syslog = logs.get("syslog", "")
        
        # Get workload context
        suspected_workloads = []
        if prom_api_client:
            try:
                suspected_workloads = get_gpu_workloads_on_node_at_time(
                    parsed_alert['node_name'], 
                    parsed_alert['event_timestamp']
                )
            except Exception as e:
                logger.warning(f"Failed to get workload context: {e}")
        
        # Check GPU availability
        gpu_availability = {}
        if prom_api_client:
            try:
                gpu_availability = check_gpu_availability(
                    parsed_alert['node_name'],
                    parsed_alert['event_timestamp']
                )
            except Exception as e:
                logger.warning(f"Failed to check GPU availability: {e}")
                gpu_availability = {"gpu_status": f"GPU check failed: {str(e)}"}
        
        # Collect DCGM metrics for enhanced GPU analysis
        dcgm_metrics = {}
        if prom_api_client:
            try:
                dcgm_metrics = collect_dcgm_metrics(
                    parsed_alert['node_name'],
                    parsed_alert['event_timestamp']
                )
            except Exception as e:
                logger.warning(f"Failed to collect DCGM metrics: {e}")
                dcgm_metrics = {
                    "metrics_available": False,
                    "error": f"DCGM collection failed: {str(e)}",
                    "anomalies": []
                }
        
        # Generate RCA using LLM
        prompt = construct_llm_prompt(parsed_alert, kern_log, dmesg_log, syslog, suspected_workloads, gpu_availability, dcgm_metrics)
        rca_result = call_llm_for_rca(prompt, parsed_alert, suspected_workloads)
        
        # Check if auto-cordoning is needed
        auto_cordon_status = {"attempted": False, "successful": False, "reason": ""}
        
        if not ENABLE_AUTO_CORDON:
            logger.info(f"Auto-cordoning disabled for node '{parsed_alert['node_name']}' (ENABLE_AUTO_CORDON=false)")
        else:
            should_cordon, cordon_reason = should_auto_cordon(rca_result, gpu_availability)
            
            if should_cordon:
                logger.info(f"Auto-cordoning triggered for node '{parsed_alert['node_name']}': {cordon_reason}")
                auto_cordon_status["attempted"] = True
                auto_cordon_status["reason"] = cordon_reason
                
                success = cordon_node(parsed_alert['node_name'], cordon_reason)
                auto_cordon_status["successful"] = success
                
                if success:
                    logger.info(f"Successfully auto-cordoned node '{parsed_alert['node_name']}'")
                    # Add cordoning info to RCA result for Slack notification
                    if "Recovery Steps" in rca_result and isinstance(rca_result["Recovery Steps"], list):
                        rca_result["Recovery Steps"].insert(0, f"✅ AUTOMATED: Node has been cordoned to prevent new workloads")
                else:
                    logger.error(f"Failed to auto-cordon node '{parsed_alert['node_name']}'")
                    if "Recovery Steps" in rca_result and isinstance(rca_result["Recovery Steps"], list):
                        rca_result["Recovery Steps"].insert(0, f"❌ AUTO-CORDON FAILED: Manual intervention required to cordon node")
            else:
                logger.info(f"Auto-cordoning not triggered for node '{parsed_alert['node_name']}': {cordon_reason}")
                auto_cordon_status["attempted"] = False
                auto_cordon_status["reason"] = cordon_reason
        
        # Cache the RCA result for future deduplication
        cache_rca_result(cache_key, rca_result)
        
        # Send to Slack
        if SLACK_WEBHOOK_URL:
            slack_message = format_slack_message(parsed_alert, rca_result, suspected_workloads, gpu_availability, auto_cordon_status, dcgm_metrics)
            send_to_slack(slack_message, SLACK_WEBHOOK_URL)
        
        processing_time = time.time() - start_time
        logger.info(f"New RCA processing completed in {processing_time:.2f} seconds")
        
        # Record processing duration metric
        if "processing_duration" in otel_metrics_instruments:
            otel_metrics_instruments["processing_duration"].record(processing_time, {
                "node_name": node_name,
                "cached": "false"
            })
        
    except Exception as e:
        logger.error(f"RCA processing failed: {e}")
        raise


# --- Flask App ---
app = Flask(__name__)
otel_prometheus_registry = setup_otel_metrics_for_prometheus()
# Use DispatcherMiddleware to serve Prometheus metrics on /metrics
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app(registry=otel_prometheus_registry) 
})


# --- Core Functions ---

def parse_alertmanager_payload(payload):
    logger.info("Parsing Alertmanager payload.")
    logger.debug(f"Received payload type: {type(payload)}")

    alert_object = None
    webhook_schema_version = None # To store the webhook schema version

    if isinstance(payload, list):
        if payload: # Check if the list is not empty
            logger.warning("Payload is a list. Assuming the first element is the primary notification object or a direct alert.")
            potential_notification_or_alert_data = payload[0]
            if not isinstance(potential_notification_or_alert_data, dict):
                raise RcaServiceError(
                    f"Invalid payload structure: first element of list payload is not a dictionary, but {type(potential_notification_or_alert_data)}."
                )
            
            # Attempt to get the webhook schema version from this first object
            webhook_schema_version = potential_notification_or_alert_data.get("version")

            if 'alerts' in potential_notification_or_alert_data and isinstance(potential_notification_or_alert_data['alerts'], list) and potential_notification_or_alert_data['alerts']:
                logger.info("Found 'alerts' array in the first list element. Processing as standard Alertmanager webhook.")
                alert_object = potential_notification_or_alert_data['alerts'][0] # Process the first alert
            elif 'labels' in potential_notification_or_alert_data and 'annotations' in potential_notification_or_alert_data: 
                logger.info("First list element appears to be a direct alert object (no 'alerts' wrapper).")
                alert_object = potential_notification_or_alert_data
                if webhook_schema_version:
                    logger.debug(f"Payload was a list of direct alerts; top-level 'version' field ({webhook_schema_version}) from the first element might not represent a standard webhook schema version in this context.")
            else:
                raise RcaServiceError(
                    "Invalid payload structure: first list element is a dictionary but neither a standard Alertmanager webhook nor a recognizable direct alert."
                )
        else: # Payload is an empty list
            raise RcaServiceError("Invalid payload structure: received an empty list.")
    elif isinstance(payload, dict):
        potential_notification_or_alert_data = payload
        # Attempt to get the webhook schema version
        webhook_schema_version = potential_notification_or_alert_data.get("version")

        if 'alerts' in potential_notification_or_alert_data and isinstance(potential_notification_or_alert_data['alerts'], list) and potential_notification_or_alert_data['alerts']:
            logger.info("Payload is a dictionary with an 'alerts' array. Processing as standard Alertmanager webhook.")
            alert_object = potential_notification_or_alert_data['alerts'][0] # Process the first alert
        elif 'labels' in potential_notification_or_alert_data and 'annotations' in potential_notification_or_alert_data: 
            logger.info("Payload is a dictionary that appears to be a direct alert object (no 'alerts' wrapper).")
            alert_object = potential_notification_or_alert_data
            if webhook_schema_version:
                 logger.debug(f"Payload was a direct alert dictionary; 'version' field ({webhook_schema_version}) might not represent a standard webhook schema version in this context.")
        else:
            raise RcaServiceError(
                "Invalid payload structure: payload is a dictionary but neither a standard Alertmanager webhook nor a recognizable direct alert."
            )
    else:
        raise RcaServiceError(
            f"Invalid payload structure: expected a dictionary or a list, but received {type(payload)}."
        )

    if not alert_object or not isinstance(alert_object, dict):
        raise RcaServiceError("Failed to extract a valid alert object from the payload.")

    labels = alert_object.get('labels', {})
    annotations = alert_object.get('annotations', {})

    node_name = labels.get('node') or labels.get('nodename') or labels.get('instance') or labels.get('Hostname')
    xid_error_code = labels.get('err_code') or annotations.get('err_code') # Corrected from err_code
    
    alert_summary = annotations.get('summary', 'N/A')
    alert_description = annotations.get('description', 'N/A')
    alert_name = labels.get('alertname', 'N/A')
    alert_severity = labels.get('severity', 'N/A')
    starts_at_str = alert_object.get('startsAt')

    if not node_name:
        raise RcaServiceError("Node name not found in alert payload.")
    if not xid_error_code:
        logger.warning("XID error code not explicitly found in alert. Will rely on logs.")

    event_timestamp = datetime.now(timezone.utc) 
    if starts_at_str:
        try:
            if starts_at_str.endswith('Z'):
                temp_ts_str = starts_at_str[:-1] 
                if '.' in temp_ts_str:
                    parts = temp_ts_str.split('.')
                    fractional = parts[1][:6]
                    temp_ts_str = parts[0] + '.' + fractional
                    event_timestamp = datetime.strptime(temp_ts_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
                else:
                    event_timestamp = datetime.strptime(temp_ts_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            else: 
                 event_timestamp = datetime.fromisoformat(starts_at_str)
                 if event_timestamp.tzinfo is None: 
                     logger.warning(f"Parsed timestamp {starts_at_str} is naive. Assuming UTC for now.")
                     event_timestamp = event_timestamp.replace(tzinfo=timezone.utc)
            logger.info(f"Parsed alert startsAt: {starts_at_str} to {event_timestamp}")
        except ValueError as e:
            logger.error(f"Error parsing startsAt timestamp '{starts_at_str}': {e}. Using current time as fallback.")
    else:
        logger.warning("No 'startsAt' field in alert. Using current time for event timestamp.")

    parsed_data = {
        "node_name": node_name, "xid_error_code": xid_error_code,
        "alert_name": alert_name, "summary": alert_summary,
        "description": alert_description, "severity": alert_severity,
        "event_timestamp": event_timestamp, # This is the 'end time' of our query window
        "raw_labels": labels, "raw_annotations": annotations,
        "webhook_schema_version": webhook_schema_version # Added webhook version
    }
    logger.info(
        f"Parsed alert data: Node={node_name}, XID={xid_error_code}, "
        f"Timestamp={event_timestamp}, WebhookVersion={webhook_schema_version}"
    )
    return parsed_data

def get_gpu_workloads_on_node_at_time(node_name, end_query_timestamp_dt):
    global prom_api_client
    if not prom_api_client:
        logger.warning("Prometheus API client (for context query) not available. Skipping workload identification.")
        return []

    query_timestamp_unix = int(end_query_timestamp_dt.timestamp())
    window_duration_str = f"{WORKLOAD_QUERY_WINDOW_MINUTES}m"
    start_query_window_dt = end_query_timestamp_dt - timedelta(minutes=WORKLOAD_QUERY_WINDOW_MINUTES)

    logger.info(f"Querying Prometheus for GPU workloads on node '{node_name}' active during window: "
                f"{start_query_window_dt.isoformat()} to {end_query_timestamp_dt.isoformat()} UTC. "
                f"Query evaluated at Unix timestamp: {query_timestamp_unix}.")
    
    query = (
        f'group by (pod, namespace) ('
        f'  (max_over_time(kube_pod_container_resource_requests{{resource=~"nvidia_com.*", node="{node_name}"}}[{window_duration_str}])))'
    )
    
    workloads = []
    query_start_time = time.time()
    try:
        logger.info(f"Executing PromQL query: {query} (evaluated at time {query_timestamp_unix})")
        result = prom_api_client.custom_query(query=query, params={'time': query_timestamp_unix})
        
        for metric in result:
            pod_name = metric['metric'].get('pod')
            namespace = metric['metric'].get('namespace')
            if pod_name and namespace:
                workloads.append({"namespace": namespace, "pod_name": pod_name})
        logger.info(f"Found {len(workloads)} potential GPU workloads on node '{node_name}' active during the window: {workloads}")
    except PrometheusApiClientException as e:
        logger.error(f"Prometheus query for workloads failed: {e}")
    except Exception as e_gen:
        logger.error(f"Unexpected error during Prometheus workload query: {e_gen}")
    finally:
        query_duration = time.time() - query_start_time
        if "workload_query_duration" in otel_metrics_instruments:
            otel_metrics_instruments["workload_query_duration"].record(query_duration, {"node_name": node_name})
    return workloads


def fetch_node_log_sdk(node_name, log_file_path_on_host=None, command_to_run=None):
    """
    Fetches logs from a node by creating a temporary pod, executing a command, and deleting the pod.
    Uses the Kubernetes Python SDK directly, without calling kubectl.
    """
    global k8s_core_v1_api

    if not k8s_core_v1_api:
        raise RcaServiceError("Kubernetes CoreV1Api not initialized.")

    if not log_file_path_on_host and not command_to_run:
        raise ValueError("Either log_file_path_on_host or command_to_run must be provided for fetch_node_log_sdk.")

    pod_name_suffix = uuid.uuid4().hex[:8]
    # Ensure pod names are DNS-1123 compliant (lowercase alphanumeric, '-', '.')
    safe_node_name = node_name.lower().replace('.', '-')
    thread_name = threading.current_thread().name.lower().replace('-', '')
    pod_name = f"log-fetch-{safe_node_name}-{thread_name}-{pod_name_suffix}"[:63] # Max length for pod name
    log_description = ""

    exec_command_in_pod = []
    if log_file_path_on_host:
        if not log_file_path_on_host.startswith("/var/log/"):
            raise ValueError("log_file_path_on_host must be a path within /var/log/")
        
        path_inside_pod = f"/host{log_file_path_on_host}" # Path inside the pod
        exec_command_in_pod = ["tail", "-n", LOG_POD_DEFAULT_TAIL_LINES, path_inside_pod]
        log_description = f"log file '{log_file_path_on_host}' using '{' '.join(exec_command_in_pod)}'"
    elif command_to_run:
        exec_command_in_pod = command_to_run
        log_description = f"command '{' '.join(exec_command_in_pod)}'"

    volume_name = "host-var-log-volume"
    pod_manifest = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=pod_name, labels={"app": "seneca-rca-log-fetcher"}),
        spec=client.V1PodSpec(
            node_name=node_name,
            restart_policy="Never",
            host_pid=True,  # Share host PID namespace
            host_network=True,  # Share host network namespace
            security_context=client.V1PodSecurityContext(
                run_as_user=0,
                run_as_group=0,
                fs_group=0
            ),
            containers=[
                client.V1Container(
                    name="log-fetch-container",
                    image=LOG_POD_IMAGE,
                    image_pull_policy=LOG_POD_PULL_POLICY,
                    command=["tail", "-f", "/dev/null"], # Keep container alive
                    volume_mounts=[
                        client.V1VolumeMount(
                            name=volume_name,
                            mount_path="/host/var/log",
                            read_only=True
                        )
                    ],
                    security_context=client.V1SecurityContext(
                        privileged=True # Running as privileged
                    )
                )
            ],
            volumes=[
                client.V1Volume(
                    name=volume_name,
                    host_path=client.V1HostPathVolumeSource(path="/var/log")
                )
            ],
            termination_grace_period_seconds=5,
            # tolerations=[client.V1Toleration(operator="Exists")] # If needed for tainted nodes
        )
    )

    try:
        logger.info(f"SDK: Creating pod '{pod_name}' on node '{node_name}' to fetch {log_description}.")
        # --- SDK CALL: Create Pod ---
        k8s_core_v1_api.create_namespaced_pod(namespace=LOG_POD_NAMESPACE, body=pod_manifest)

        start_wait_time = time.time()
        pod_phase = ""
        while time.time() - start_wait_time < LOG_POD_CREATION_TIMEOUT_SECONDS:
            # --- SDK CALL: Read Pod Status ---
            pod_status_resp = k8s_core_v1_api.read_namespaced_pod_status(name=pod_name, namespace=LOG_POD_NAMESPACE)
            pod_phase = pod_status_resp.status.phase
            if pod_phase == "Running":
                if pod_status_resp.status.container_statuses and \
                   all(cs.ready for cs in pod_status_resp.status.container_statuses):
                    logger.info(f"SDK: Pod '{pod_name}' is running and containers are ready.")
                    break
            elif pod_phase in ["Failed", "Unknown", "Succeeded"]:
                pod_logs_for_debug = "N/A"
                try:
                    # --- SDK CALL: Read Pod Logs (for debugging the fetcher pod itself) ---
                    pod_logs_for_debug = k8s_core_v1_api.read_namespaced_pod_log(name=pod_name, namespace=LOG_POD_NAMESPACE, container="log-fetch-container")
                except Exception: pass
                raise RcaServiceError(f"SDK: Pod '{pod_name}' reached phase '{pod_phase}' prematurely. Pod logs: {pod_logs_for_debug[:500]}")
            time.sleep(2)
        else:
            raise RcaServiceError(f"SDK: Timeout waiting for pod '{pod_name}' to be running. Last phase: {pod_phase}")

        logger.info(f"SDK: Executing in pod '{pod_name}': {' '.join(exec_command_in_pod)}")
        
        # --- SDK CALL: Exec in Pod ---
        exec_response = stream(
            k8s_core_v1_api.connect_get_namespaced_pod_exec,
            name=pod_name,
            namespace=LOG_POD_NAMESPACE,
            command=exec_command_in_pod,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=True
        )
        
        logger.info(f"SDK: Exec response type: {type(exec_response)}, length: {len(exec_response) if exec_response else 'None'}")
        if exec_response:
            logger.info(f"SDK: First 200 chars of response: {exec_response[:200]}")
            logger.info(f"SDK: Successfully fetched {log_description} from pod '{pod_name}'. Length: {len(exec_response)} bytes.")
            return exec_response
        else:
            logger.warning(f"SDK: Exec returned empty response for pod '{pod_name}'")
            raise RcaServiceError(f"SDK: Empty response from exec in pod '{pod_name}'")

    except ApiException as e:
        err_msg = f"SDK: Kubernetes API error with pod '{pod_name}' for {log_description}: {e.reason} (Status: {e.status}) Body: {e.body}"
        logger.error(err_msg)
        raise RcaServiceError(err_msg) from e
    except RcaServiceError:
        raise
    except Exception as e:
        err_msg = f"SDK: Unexpected error during SDK log fetch with pod '{pod_name}' for {log_description}: {e}"
        logger.error(err_msg, exc_info=True)
        raise RcaServiceError(err_msg) from e
    finally:
        # Clean up the pod after log fetching
        try:
            logger.info(f"SDK: Attempting to delete pod '{pod_name}' in namespace '{LOG_POD_NAMESPACE}'.")
            # --- SDK CALL: Delete Pod ---
            # Check if pod exists before trying to delete
            try:
                k8s_core_v1_api.read_namespaced_pod_status(name=pod_name, namespace=LOG_POD_NAMESPACE)
                k8s_core_v1_api.delete_namespaced_pod(
                    name=pod_name,
                    namespace=LOG_POD_NAMESPACE,
                    body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=5)
                )
                logger.info(f"SDK: Pod '{pod_name}' delete request issued.")
            except ApiException as e_read:
                if e_read.status == 404:
                    logger.info(f"SDK: Pod '{pod_name}' not found for deletion, likely already deleted or failed to create.")
                else:
                    logger.error(f"SDK: API error checking status of pod '{pod_name}' before deletion: {e_read.reason}", exc_info=True)
            except Exception as e_check_del:
                 logger.error(f"SDK: Unexpected error checking status of pod '{pod_name}' before deletion: {e_check_del}", exc_info=True)
        except ApiException as e_del_api:
            logger.error(f"SDK: API error deleting pod '{pod_name}': {e_del_api.reason}", exc_info=True)
        except Exception as e_del_generic:
            logger.error(f"SDK: Unexpected error deleting pod '{pod_name}': {e_del_generic}", exc_info=True)


def construct_llm_prompt(alert_data, kern_log, dmesg_log, syslog, suspected_workloads, gpu_availability, dcgm_metrics):
    logger.info("Constructing plain text prompt for LLM.")
    max_log_len = 15000 
    kern_log_snippet = kern_log[-max_log_len:] if kern_log else "N/A"
    dmesg_log_snippet = dmesg_log[-max_log_len:] if dmesg_log else "N/A"
    syslog_snippet = syslog[-max_log_len:] if syslog else "N/A"

    alert_labels_json = json.dumps(alert_data.get("raw_labels", {}), indent=2)
    alert_annotations_json = json.dumps(alert_data.get("raw_annotations", {}), indent=2)
    
    xid_info = ""
    if alert_data.get("xid_error_code"):
         xid_info = f"Explicit XID Code from alert: {alert_data['xid_error_code']}"

    end_query_window_dt = alert_data.get('event_timestamp', datetime.now(timezone.utc))
    start_query_window_dt = end_query_window_dt - timedelta(minutes=WORKLOAD_QUERY_WINDOW_MINUTES)

    workloads_text = "No specific GPU workloads identified on the node during the relevant window."
    if suspected_workloads:
        workloads_list_str = "\n".join([f"- Namespace: {w['namespace']}, Pod: {w['pod_name']}" for w in suspected_workloads])
        workloads_text = (f"The following GPU workloads were running on node {alert_data['node_name']} "
                          f"during the window from {start_query_window_dt.isoformat()} UTC to {end_query_window_dt.isoformat()} UTC "
                          f"and might be related:\n{workloads_list_str}")

    gpu_status_text = gpu_availability.get('gpu_status', 'GPU status unknown')

    dcgm_summary = ""
    dcgm_anomalies_text = ""
    if dcgm_metrics.get("metrics_available"):
        dcgm_summary = dcgm_metrics.get("summary", "DCGM metrics collected but summary unavailable")
        
        # Format anomalies for the prompt
        anomalies = dcgm_metrics.get("anomalies", [])
        if anomalies:
            anomaly_messages = [anomaly["message"] for anomaly in anomalies]
            dcgm_anomalies_text = f"\n\nDCGM Anomalies Detected ({len(anomalies)} total):\n" + "\n".join([f"- {msg}" for msg in anomaly_messages])
        else:
            dcgm_anomalies_text = "\n\nDCGM Anomalies: None detected - all metrics within normal thresholds"
    else:
        dcgm_summary = dcgm_metrics.get("error", "DCGM metrics not available")
        dcgm_anomalies_text = "\n\nDCGM Anomalies: Cannot analyze - metrics collection failed"

    prompt = f"""You are an expert SRE specializing in Nvidia GPU infrastructure and troubleshooting. An XID error event has occurred on a Kubernetes node. Your task is to perform a Root Cause Analysis (RCA) based on the provided alert data, system logs, potentially related workloads, and comprehensive GPU telemetry data.

Alert Data:
Alert Name: {alert_data.get('alert_name', 'N/A')}
Severity: {alert_data.get('severity', 'N/A')}
Node: {alert_data.get('node_name', 'N/A')}
Summary: {alert_data.get('summary', 'N/A')}
Description: {alert_data.get('description', 'N/A')}
Alert Fired At: {end_query_window_dt.isoformat()} UTC
{xid_info}
Raw Labels:
{alert_labels_json}
Raw Annotations:
{alert_annotations_json}

Potentially Related Workloads:
{workloads_text}

Node Logs (recent snippets, ideally covering the time leading up to and around {end_query_window_dt.isoformat()} UTC):
Kern.log Snippet:
{kern_log_snippet}

Dmesg.log Snippet:
{dmesg_log_snippet}

Syslog Snippet:
{syslog_snippet}

GPU Availability:
{gpu_status_text}

DCGM GPU Telemetry (30-minute window before alert):
{dcgm_summary}{dcgm_anomalies_text}

Analysis Request:

1.  XID Error Interpretation: If an XID code is evident from logs or alert, briefly explain what it generally indicates. If not explicit, state that.

2.  Correlate Logs & Workloads: Identify messages in kern.log, dmesg, or syslog that directly correlate with potential GPU errors or the XID event. If suspected workloads are listed, consider if their nature or timing within the window could be relevant. Point out specific timestamps or log patterns. Look for Nvidia-driver messages, NVRM, XID messages, hardware errors, overheating, power issues, or application-level CUDA errors from the workloads if logs suggest it.

3.  GPU Availability Impact: Based on the GPU availability status provided, assess whether the XID error has caused GPU loss or unavailability. If GPUs are missing compared to historical values, factor this into your root cause analysis.

4.  DCGM Telemetry Analysis: Analyze the DCGM metrics and anomalies provided:
   - Temperature anomalies: Critical (>95°C) or high (>85°C) temperatures indicate thermal issues
   - Memory utilization: High utilization (>90%) may indicate memory pressure or leaks
   - NVLink bandwidth: Low bandwidth (<10 GB/s) suggests interconnect issues
   - PCIe traffic: High traffic (>15 GB/s) may indicate bus saturation
   - Thermal/Power violations: Any violations indicate hardware stress or failure
   - Correlate these metrics with the XID error timing and severity

5.  Evaluate PCIe / Link Errors: The logs may include corrected or uncorrected PCIe errors (e.g., BadTLP, RxErr). Explicitly comment on these messages, whether they indicate a flapping PCIe link, how serious they are, and how they influence criticality. Cross-reference with DCGM PCIe traffic data. Mention if further hardware diagnostics (e.g., reseating GPU, checking riser cables) are warranted.

6.  Potential Root Cause(s): Based on the XID error, log evidence, workload context, PCIe error assessment, GPU availability status, and DCGM telemetry anomalies, hypothesize the most likely root cause(s). Consider:
   - Hardware issues (GPU card fault, power supply, cooling, PCIe bus) - correlate with temperature, power violations
   - Software issues (Nvidia driver version/corruption, CUDA library mismatch, workload application bugs exhausting resources or causing invalid operations) - correlate with memory utilization, workload patterns
   - Resource contention or misconfiguration - correlate with high utilization metrics
   - Thermal management issues - correlate with temperature anomalies and thermal violations

7.  Criticality Assessment: On a scale of Low, Medium, High, Critical, how severe is this issue for the node's GPU functionality and potentially the workload? Factor in actual GPU loss if detected, DCGM anomaly severity (critical temperature, violations), and potential for cascading failures. Justify based on telemetry data.

8.  Recommended Recovery Steps: Provide a concise, actionable list of steps. If GPU loss is detected or critical DCGM anomalies are present, prioritize immediate actions (GPU reset/node reboot, thermal management). If thermal violations are detected, include cooling system checks. Prioritize based on GPU availability status and DCGM anomaly severity.

9.  Confidence Score: Provide a confidence score (0-100%) in your overall RCA, factoring in the quality and consistency of log data, DCGM telemetry, and correlation between different data sources.

Output Format (Strictly follow this):

Summary: (One or two-sentence executive summary including key DCGM findings if relevant)
XID Interpretation: (Your interpretation or "XID not explicitly identified")
Log & Workload Correlation: (Key log snippets, workload relevance within the identified time window)
DCGM Analysis: (Key findings from GPU telemetry, anomalies, and their correlation with the XID error)
Potential Root Cause(s): (List ONLY the 2 most probable causes, ranked by likelihood, incorporating DCGM evidence)
Criticality: (Low/Medium/High/Critical) - (1-2 sentence justification including DCGM anomaly impact)
Recovery Steps:
1.  (Step 1 - most critical, informed by DCGM anomalies)
2.  (Step 2 - second most critical)
3.  (Step 3 - third most critical, if needed)
(Maximum 3-4 steps only, focus on immediate actions based on telemetry)
Confidence: (0-100)%

Self-Challenge (Internal Thought Process for you, the LLM - do not include in the final response to be sent to Slack, but use it to refine your answer above):
Do the DCGM metrics support or contradict the log evidence?
Could the identified workloads be victims rather than causes, especially given the telemetry data?
What if the logs are sparse but DCGM shows clear anomalies? How would the RCA change?
Are there any conflicting pieces of information between logs, GPU availability, and DCGM metrics?
How would the RCA differ if multiple XIDs were reported or if this is a recurring issue with consistent DCGM patterns?
Do the thermal/power violations correlate with the XID timing, suggesting a hardware root cause?
"""
    return prompt

def parse_llm_response_to_dict(llm_text_response):
    parsed_rca = {}
    current_key = None
    current_value_lines = []
    headers = {
        "Summary:": "Summary", 
        "XID Interpretation:": "XID Interpretation",
        "Log & Workload Correlation:": "Log & Workload Correlation",
        "DCGM Analysis:": "DCGM Analysis",
        "Potential Root Cause(s):": "Potential Root Cause(s)", 
        "Criticality:": "Criticality",
        "Recovery Steps:": "Recovery Steps", 
        "Confidence:": "Confidence"
    }
    header_keys = list(headers.keys())
    lines = llm_text_response.strip().split('\n')
    active_header_key = None
    
    for line in lines:
        stripped_line = line.strip()
        matched_header = None
        for h_key in header_keys:
            if stripped_line.startswith(h_key):
                matched_header = h_key
                break
        
        if matched_header:
            if active_header_key and current_value_lines:
                dict_key = headers[active_header_key]
                if dict_key == "Recovery Steps":
                    parsed_rca[dict_key] = [l.strip().lstrip('0123456789.').strip() for l in current_value_lines if l.strip()]
                else:
                    parsed_rca[dict_key] = "\n".join(current_value_lines).strip()
            active_header_key = matched_header
            current_value_lines = [stripped_line.replace(active_header_key, "").strip()]
        elif active_header_key:
            current_value_lines.append(stripped_line)

    if active_header_key and current_value_lines:
        dict_key = headers[active_header_key]
        if dict_key == "Recovery Steps":
            parsed_rca[dict_key] = [l.strip().lstrip('0123456789.').strip() for l in current_value_lines if l.strip()]
        else:
            parsed_rca[dict_key] = "\n".join(current_value_lines).strip()

    if "Criticality" in parsed_rca:
        # Clean up the criticality text by removing line breaks and extra spaces
        criticality_text = parsed_rca["Criticality"].replace('\n', ' ').replace('\r', ' ')
        # Remove multiple spaces
        criticality_text = ' '.join(criticality_text.split())
        
        crit_level = None
        crit_just = None
        if '-' in criticality_text:
            crit_level, crit_just = [p.strip() for p in criticality_text.split('-', 1)]
        else:
            # Fallback: first word = level, rest = justification
            parts = criticality_text.split(' ', 1)
            crit_level = parts[0]
            crit_just = parts[1].strip() if len(parts) > 1 else "N/A"
        parsed_rca["Criticality"] = crit_level
        parsed_rca["Criticality_Justification"] = crit_just or "N/A"
    if "Confidence" in parsed_rca:
        # Clean up confidence text and extract just the number
        confidence_text = parsed_rca["Confidence"].replace('\n', ' ').replace('\r', ' ')
        confidence_text = ' '.join(confidence_text.split())  # Remove multiple spaces
        parsed_rca["Confidence"] = confidence_text.replace('%', '').strip()
    logger.info(f"Parsed LLM response: {json.dumps(parsed_rca, indent=2)}")
    return parsed_rca


def call_llm_for_rca(prompt_text, alert_data, suspected_workloads): 
    global openai_client
    if not openai_client:
        logger.error("OpenAI client not initialized. Cannot perform RCA without LLM.")
        raise RcaServiceError("OpenAI client not configured - RCA analysis requires valid OpenAI API key")

    logger.info(f"Sending prompt to OpenAI model: {OPENAI_MODEL}")
    llm_call_start_time = time.time()
    
    try:
        chat_completion = openai_client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are an expert SRE specializing in Nvidia GPU infrastructure. Follow the output format strictly."},
                {"role": "user", "content": prompt_text}
            ],
            model=OPENAI_MODEL
        )
        llm_response_text = chat_completion.choices[0].message.content
        logger.info(f"Received LLM response. Tokens used: {getattr(chat_completion, 'usage', 'N/A')}")
        parsed_rca_result = parse_llm_response_to_dict(llm_response_text)
        return parsed_rca_result
    except openai.APIError as e:
        logger.error(f"OpenAI API returned an API Error: {e}")
        raise RcaServiceError(f"OpenAI API Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during OpenAI API call: {e}")
        raise RcaServiceError(f"LLM call failed: {e}")
    finally:
        llm_call_duration = time.time() - llm_call_start_time
        if "llm_call_duration" in otel_metrics_instruments:
            otel_metrics_instruments["llm_call_duration"].record(llm_call_duration)


def format_slack_message(alert_data, rca_result, suspected_workloads, gpu_availability, auto_cordon_status, dcgm_metrics):
    logger.info("Formatting Slack message.")
    node_name = alert_data.get("node_name", "N/A")
    alert_name = alert_data.get("alert_name", "N/A")
    xid_code = alert_data.get("xid_error_code", "Not specified in alert")
    
    end_query_window_dt = alert_data.get('event_timestamp', datetime.now(timezone.utc))
    start_query_window_dt = end_query_window_dt - timedelta(minutes=WORKLOAD_QUERY_WINDOW_MINUTES)

    workload_text_for_slack = ""
    if suspected_workloads:
        workload_lines = [f"  • `{w['namespace']}/{w['pod_name']}`" for w in suspected_workloads]
        workload_text_for_slack = (f"*Suspected Workload(s) on Node (active during {start_query_window_dt.strftime('%H:%M:%S')} - {end_query_window_dt.strftime('%H:%M:%S')} UTC):*\n" + 
                                   "\n".join(workload_lines) + "\n")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🧠 RCA: {alert_name} on {node_name}", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn",
            "text": (f"*Summary:* {alert_data.get('summary', 'N/A')}\n"
                     f"*Fired At:* `{end_query_window_dt.isoformat()}` UTC\n"
                     f"*XID from Alert:* `{xid_code}`")}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*AI RCA Summary:*\n{rca_result.get('Summary', 'N/A')}"}}
    ]
    if workload_text_for_slack:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": workload_text_for_slack}})
    
    # Add GPU availability status
    if gpu_availability:
        gpu_status_text = gpu_availability.get('gpu_status', 'GPU status unknown')
        current_gpus = gpu_availability.get('current_gpus', 'Unknown')
        historical_gpus = gpu_availability.get('historical_gpus', 'Unknown')

        counts_known = isinstance(current_gpus, (int, float)) and isinstance(historical_gpus, (int, float))
        if counts_known:
            gpu_info_text = (f"*GPU Availability Status:* {gpu_status_text}\n"
                              f"*Current GPUs:* *{current_gpus}* | *Historical GPUs (1h ago):* *{historical_gpus}*")
        else:
            gpu_info_text = f"*GPU Availability Status:* {gpu_status_text}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": gpu_info_text}})
    
    # Add DCGM metrics information
    if dcgm_metrics and dcgm_metrics.get("metrics_available"):
        dcgm_summary = dcgm_metrics.get("summary", "DCGM metrics collected")
        anomalies = dcgm_metrics.get("anomalies", [])
        
        dcgm_text = f"*DCGM GPU Telemetry:* {dcgm_summary}"
        
        if anomalies:
            # Group anomalies by severity
            critical_anomalies = [a for a in anomalies if a.get("severity") == "critical"]
            high_anomalies = [a for a in anomalies if a.get("severity") == "high"]
            medium_anomalies = [a for a in anomalies if a.get("severity") == "medium"]
            
            anomaly_text = "\n*🚨 DCGM Anomalies Detected:*"
            
            if critical_anomalies:
                anomaly_text += "\n*CRITICAL:*"
                for anomaly in critical_anomalies:
                    anomaly_text += f"\n  • {anomaly['message']}"
            
            if high_anomalies:
                anomaly_text += "\n*HIGH:*"
                for anomaly in high_anomalies:
                    anomaly_text += f"\n  • {anomaly['message']}"
            
            if medium_anomalies:
                anomaly_text += "\n*MEDIUM:*"
                for anomaly in medium_anomalies:
                    anomaly_text += f"\n  • {anomaly['message']}"
            
            dcgm_text += anomaly_text
        
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": dcgm_text}})
    elif dcgm_metrics and not dcgm_metrics.get("metrics_available"):
        dcgm_error = dcgm_metrics.get("error", "DCGM metrics collection failed")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*DCGM GPU Telemetry:* ⚠️ {dcgm_error}"}})
    
    # Add auto-cordoning status - always show if feature is enabled
    if ENABLE_AUTO_CORDON and auto_cordon_status:
        if auto_cordon_status.get("attempted"):
            if auto_cordon_status.get("successful"):
                cordon_text = f"🤖 *Automated Action Taken:*\n✅ Node `{node_name}` has been automatically cordoned\n*Reason:* {auto_cordon_status.get('reason', 'N/A')}"
            else:
                cordon_text = f"🤖 *Automated Action Failed:*\n❌ Failed to cordon node `{node_name}`\n*Reason:* {auto_cordon_status.get('reason', 'N/A')}\n⚠️ Manual intervention required"
        else:
            cordon_text = f"🤖 *Automated Evaluation:*\n⏸️ Node `{node_name}` was not cordoned\n*Reason:* {auto_cordon_status.get('reason', 'N/A')}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": cordon_text}})
    elif not ENABLE_AUTO_CORDON:
        cordon_text = f"🤖 *Automated Action:*\n⚙️ Auto-cordoning is disabled (ENABLE_AUTO_CORDON=false)"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": cordon_text}})
    
    blocks.append({"type": "divider"})
    
    criticality_justification = rca_result.get('Criticality_Justification', 'N/A')
    criticality_level = rca_result.get('Criticality', 'N/A')
    
    # Format criticality with proper Slack markdown
    if criticality_justification != 'N/A':
        criticality_text = f"*Criticality:* *{criticality_level}*\n{criticality_justification}"
    else:
        criticality_text = f"*Criticality:* *{criticality_level}*"

    details_text = (
        f"*XID Interpretation:*\n{rca_result.get('XID Interpretation', 'N/A')}\n\n"
        f"*Log & Workload Correlation Highlights:*\n```\n{rca_result.get('Log & Workload Correlation', 'N/A')}\n```\n\n"
        f"*DCGM Analysis:*\n{rca_result.get('DCGM Analysis', 'N/A')}\n\n"
        f"*Potential Root Cause(s):*\n{rca_result.get('Potential Root Cause(s)', 'N/A')}\n\n"
        f"{criticality_text}\n\n"
    )
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": details_text}})

    recovery_steps_list = rca_result.get('Recovery Steps', [])
    recovery_steps_text = f"*Recommended Recovery Steps:*\n{str(recovery_steps_list) if recovery_steps_list else 'N/A'}"
    if isinstance(recovery_steps_list, list) and recovery_steps_list:
        recovery_steps_text = "*Recommended Recovery Steps:*\n" + "\n".join([f"{i+1}. {step}" for i, step in enumerate(recovery_steps_list)])
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": recovery_steps_text}})
    
    blocks.append({"type": "divider"})
    blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"*AI Confidence:* {rca_result.get('Confidence', 'N/A')}%"},
            {"type": "mrkdwn", "text": f"RCA Generated at: {datetime.utcnow().isoformat(timespec='seconds')}Z"}
        ]})
    fallback_text = f"XID Error RCA for {alert_name} on Node {node_name}: {rca_result.get('Summary', 'N/A')}"
    return {"text": fallback_text, "blocks": blocks}


def send_to_slack(message_payload, slack_url):
    if not slack_url:
        logger.error("SLACK_WEBHOOK_URL is not set. Cannot send message.")
        raise RcaServiceError("Slack webhook URL not configured.")
    logger.info(f"Sending message to Slack webhook.")
    try:
        response = requests.post(slack_url, json=message_payload, timeout=10)
        response.raise_for_status()
        logger.info("Successfully sent message to Slack.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending message to Slack: {e}")
        raise RcaServiceError(f"Failed to send Slack message: {e}")

# --- Flask Webhook Route ---
@app.route('/webhook', methods=['POST'])
def alert_webhook():
    if "alerts_received" in otel_metrics_instruments: 
        otel_metrics_instruments["alerts_received"].add(1)
    else:
        logger.warning("OTel instruments not available for alerts_received count.")

    try:
        payload = request.json
        logger.info("Received webhook payload.")
        
        # Parse payload to extract alerts
        if isinstance(payload, list):
            # Direct list of alerts
            alerts = payload
        elif isinstance(payload, dict) and 'alerts' in payload:
            # Standard Alertmanager webhook format
            alerts = payload['alerts']
        else:
            # Single alert object
            alerts = [payload]
        
        # Enqueue each alert for processing
        queued_count = 0
        for alert in alerts:
            if enqueue_alert(alert):
                queued_count += 1
        
        logger.info(f"Successfully queued {queued_count} out of {len(alerts)} alerts for processing")
        
        return jsonify({
            "status": "success", 
            "message": f"Queued {queued_count} alerts for processing",
            "queued_alerts": queued_count,
            "total_alerts": len(alerts)
        }), 200

    except Exception as e:
        logger.exception("Error processing webhook payload.")
        if "failed_rca" in otel_metrics_instruments:
            otel_metrics_instruments["failed_rca"].add(1, {"reason": "webhook_error"})
        return jsonify({"status": "error", "message": "Failed to process webhook payload"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    queue_size = alert_queue.qsize() if alert_queue else 0
    active_workers = len([t for t in worker_threads if t.is_alive()]) if worker_threads else 0
    cache_stats = get_cache_stats()
    
    return jsonify({
        "status": "healthy",
        "queue_size": queue_size,
        "active_workers": active_workers,
        "max_workers": MAX_WORKER_THREADS,
        "cache_stats": cache_stats,
        "cache_ttl_hours": RCA_CACHE_TTL_HOURS
    }), 200

# --- RCA Deduplication Functions ---
def generate_rca_cache_key(node_name, xid_error_code, alert_name):
    """Generate a unique cache key for RCA deduplication."""
    # Create a unique key based on node, XID error code, and alert name
    key_components = [
        str(node_name).lower(),
        str(xid_error_code).lower() if xid_error_code else "unknown",
        str(alert_name).lower()
    ]
    key_string = "|".join(key_components)
    # Use hash to create a shorter, consistent key
    return hashlib.md5(key_string.encode()).hexdigest()

def is_rca_already_processed(cache_key):
    """Check if RCA for this key was already processed recently."""
    with rca_cache_lock:
        if cache_key in rca_cache:
            cached_entry = rca_cache[cache_key]
            cached_time = cached_entry["timestamp"]
            ttl_threshold = datetime.now(timezone.utc) - timedelta(hours=RCA_CACHE_TTL_HOURS)
            
            if cached_time > ttl_threshold:
                logger.info(f"RCA cache hit for key: {cache_key}. Skipping duplicate processing.")
                
                # Record cache hit metric
                if "cache_hits" in otel_metrics_instruments:
                    otel_metrics_instruments["cache_hits"].add(1)
                
                return True, cached_entry["result"]
            else:
                # Entry expired, remove it
                del rca_cache[cache_key]
                logger.info(f"RCA cache entry expired for key: {cache_key}")
                
                # Update cache size metric
                if "cache_size" in otel_metrics_instruments:
                    otel_metrics_instruments["cache_size"].add(-1)
        
        # Record cache miss metric
        if "cache_misses" in otel_metrics_instruments:
            otel_metrics_instruments["cache_misses"].add(1)
        
        return False, None

def cache_rca_result(cache_key, rca_result):
    """Cache the RCA result for deduplication."""
    with rca_cache_lock:
        is_new_entry = cache_key not in rca_cache
        
        rca_cache[cache_key] = {
            "timestamp": datetime.now(timezone.utc),
            "result": rca_result
        }
        logger.info(f"Cached RCA result for key: {cache_key}")
        
        # Update cache size metric only for new entries
        if is_new_entry and "cache_size" in otel_metrics_instruments:
            otel_metrics_instruments["cache_size"].add(1)

def cleanup_expired_cache_entries():
    """Remove expired entries from the RCA cache."""
    ttl_threshold = datetime.now(timezone.utc) - timedelta(hours=RCA_CACHE_TTL_HOURS)
    expired_keys = []
    
    with rca_cache_lock:
        for cache_key, entry in rca_cache.items():
            if entry["timestamp"] <= ttl_threshold:
                expired_keys.append(cache_key)
        
        for key in expired_keys:
            del rca_cache[key]
    
    if expired_keys:
        logger.info(f"Cleaned up {len(expired_keys)} expired RCA cache entries")
        
        # Update cache size metric
        if "cache_size" in otel_metrics_instruments:
            otel_metrics_instruments["cache_size"].add(-len(expired_keys))

def cache_cleanup_worker():
    """Background thread to periodically clean up expired cache entries."""
    thread_name = threading.current_thread().name
    logger.info(f"RCA cache cleanup thread {thread_name} started")
    
    while not shutdown_event.is_set():
        try:
            # Wait for cleanup interval or shutdown signal
            if shutdown_event.wait(timeout=RCA_CACHE_CLEANUP_INTERVAL_MINUTES * 60):
                break  # Shutdown signal received
            
            cleanup_expired_cache_entries()
            
        except Exception as e:
            logger.error(f"Cache cleanup thread {thread_name} encountered error: {e}")
    
    logger.info(f"RCA cache cleanup thread {thread_name} shutting down")

def get_cache_stats():
    """Get statistics about the RCA cache."""
    with rca_cache_lock:
        total_entries = len(rca_cache)
        
        # Count expired entries
        ttl_threshold = datetime.now(timezone.utc) - timedelta(hours=RCA_CACHE_TTL_HOURS)
        expired_count = sum(1 for entry in rca_cache.values() if entry["timestamp"] <= ttl_threshold)
        active_count = total_entries - expired_count
        
        return {
            "total_entries": total_entries,
            "active_entries": active_count,
            "expired_entries": expired_count
        }

def check_gpu_availability(node_name, alert_timestamp):
    """
    Check current GPU availability on a node and compare with historical values.
    Returns a dictionary with current and historical GPU counts.
    """
    global prom_api_client
    if not prom_api_client:
        logger.warning("Prometheus API client not available. Skipping GPU availability check.")
        return {
            "current_gpus": "Unknown - Prometheus not available",
            "historical_gpus": "Unknown - Prometheus not available", 
            "gpu_status": "Cannot determine - Prometheus not configured"
        }

    now_utc = datetime.now(timezone.utc)
    # Guard against future timestamps in alerts (e.g., clock skew)
    if alert_timestamp > now_utc:
        logger.warning(
            f"Alert timestamp {alert_timestamp.isoformat()} is in the future relative to now {now_utc.isoformat()}. "
            "Clamping to current time for GPU availability check."
        )
        alert_timestamp = now_utc

    gpu_status = {
        "current_gpus": None,
        "historical_gpus": None,
        "gpu_status": "Unknown"
    }
    
    try:
        # Query current GPU capacity
        current_query = f'kube_node_status_capacity{{resource=~"nvidia_com_gpu", node="{node_name}"}}'
        logger.info(f"Checking current GPU availability: {current_query}")
        
        current_result = prom_api_client.custom_query(query=current_query)
        if current_result:
            gpu_status["current_gpus"] = int(float(current_result[0]['value'][1]))
        
        # Query historical GPU capacity (1 hour before alert)
        historical_timestamp = int((alert_timestamp - timedelta(hours=1)).timestamp())
        historical_result = prom_api_client.custom_query(
            query=current_query, 
            params={'time': historical_timestamp}
        )
        if historical_result:
            gpu_status["historical_gpus"] = int(float(historical_result[0]['value'][1]))
        
        # Determine GPU status with graceful handling of missing data
        cur = gpu_status["current_gpus"]
        hist = gpu_status["historical_gpus"]
        
        if cur is None:
            gpu_status["gpu_status"] = "Unknown - no current GPU metric found"
        elif hist is None:
            gpu_status["gpu_status"] = f"⚠️ Historical GPU data unavailable (current={cur})"
        else:
            if cur == hist and cur > 0:
                gpu_status["gpu_status"] = f"✅ All {cur} GPUs available (no change)"
            elif cur < hist:
                lost_gpus = hist - cur
                gpu_status["gpu_status"] = f"❌ GPU loss detected: {lost_gpus} GPU(s) lost ({hist} → {cur})"
            elif cur > hist:
                gpu_status["gpu_status"] = f"⚠️ GPU count increased: {hist} → {cur} (unexpected)"
            elif cur == 0:
                gpu_status["gpu_status"] = f"🚨 All GPUs unavailable (was {hist})"
            else:
                gpu_status["gpu_status"] = f"⚠️ GPU status unclear: current={cur}, historical={hist}"
            
        logger.info(f"GPU availability check for node '{node_name}': {gpu_status}")
        
    except PrometheusApiClientException as e:
        logger.error(f"Prometheus query for GPU availability failed: {e}")
        gpu_status["gpu_status"] = f"Error querying GPU status: {str(e)}"
        gpu_status["current_gpus"] = gpu_status.get("current_gpus", "Unknown")
        gpu_status["historical_gpus"] = gpu_status.get("historical_gpus", "Unknown")
    except Exception as e:
        logger.error(f"Unexpected error during GPU availability check: {e}")
        gpu_status["gpu_status"] = f"Unexpected error: {str(e)}"
        gpu_status["current_gpus"] = gpu_status.get("current_gpus", "Unknown")
        gpu_status["historical_gpus"] = gpu_status.get("historical_gpus", "Unknown")
    
    return gpu_status

def cordon_node(node_name, reason="Automated cordoning due to GPU XID error"):
    """
    Cordon a Kubernetes node to prevent new pods from being scheduled.
    Returns True if successful, False otherwise.
    """
    global k8s_core_v1_api
    
    if not k8s_core_v1_api:
        logger.error("Kubernetes CoreV1Api not initialized. Cannot cordon node.")
        return False
    
    try:
        # Get the current node object
        node = k8s_core_v1_api.read_node(name=node_name)
        
        # Check if already cordoned
        if node.spec.unschedulable:
            logger.info(f"Node '{node_name}' is already cordoned")
            return True
        
        # Create patch to make node unschedulable
        patch_body = {
            "spec": {
                "unschedulable": True
            },
            "metadata": {
                "annotations": {
                    "rca.sre/cordon-reason": reason,
                    "rca.sre/cordon-timestamp": datetime.now(timezone.utc).isoformat()
                }
            }
        }
        
        # Apply the patch
        k8s_core_v1_api.patch_node(name=node_name, body=patch_body)
        logger.info(f"Successfully cordoned node '{node_name}'. Reason: {reason}")
        return True
        
    except ApiException as e:
        logger.error(f"Kubernetes API error cordoning node '{node_name}': {e.reason} (Status: {e.status})")
        return False
    except Exception as e:
        logger.error(f"Unexpected error cordoning node '{node_name}': {e}")
        return False


def should_auto_cordon(rca_result, gpu_availability):
    """
    Determine if a node should be automatically cordoned based on RCA results and GPU status.
    Returns (should_cordon: bool, reason: str)
    """
    # Check confidence level
    try:
        confidence = int(rca_result.get('Confidence', '0'))
    except (ValueError, TypeError):
        confidence = 0
    
    if confidence <= AUTO_CORDON_CONFIDENCE_THRESHOLD:
        return False, f"Confidence too low: {confidence}% (threshold: >{AUTO_CORDON_CONFIDENCE_THRESHOLD}%)"
    
    # Check GPU availability
    gpu_status = gpu_availability.get('gpu_status', '')
    current_gpus = gpu_availability.get('current_gpus', 0)
    historical_gpus = gpu_availability.get('historical_gpus', 0)
    
    # Conditions for auto-cordoning
    if '❌ GPU loss detected' in gpu_status:
        return True, f"High confidence ({confidence}%) GPU loss detected: {historical_gpus} → {current_gpus} GPUs"
    elif '🚨 All GPUs unavailable' in gpu_status:
        return True, f"High confidence ({confidence}%) all GPUs unavailable (was {historical_gpus})"
    elif current_gpus == 0 and historical_gpus > 0:
        return True, f"High confidence ({confidence}%) complete GPU failure: {historical_gpus} → 0 GPUs"
    
    return False, f"No auto-cordon needed: confidence={confidence}%, no GPU issues detected"

def collect_dcgm_metrics(node_name, alert_timestamp):
    """
    Collect DCGM metrics for GPU health analysis.
    Returns a dictionary with DCGM metrics and anomaly analysis.
    """
    global prom_api_client
    if not prom_api_client:
        logger.warning("Prometheus API client not available. Skipping DCGM metrics collection.")
        return {
            "metrics_available": False,
            "error": "Prometheus not available",
            "anomalies": []
        }

    dcgm_start_time = time.time()
    
    now_utc = datetime.now(timezone.utc)
    # Guard against future timestamps
    if alert_timestamp > now_utc:
        alert_timestamp = now_utc

    # Define time window for metrics collection
    end_time = alert_timestamp
    start_time = end_time - timedelta(minutes=DCGM_METRICS_WINDOW_MINUTES)
    
    dcgm_data = {
        "metrics_available": True,
        "collection_window": {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "duration_minutes": DCGM_METRICS_WINDOW_MINUTES
        },
        "metrics": {},
        "anomalies": [],
        "summary": ""
    }
    
    # DCGM metric queries - these are the standard DCGM exporter metric names
    dcgm_queries = {
        "gpu_temperature": f'DCGM_FI_DEV_GPU_TEMP{{node="{node_name}"}}',
        "memory_utilization": f'DCGM_FI_DEV_MEM_COPY_UTIL{{node="{node_name}"}}',
        "pcie_tx_bytes": f'DCGM_FI_DEV_PCIE_TX_BYTES{{node="{node_name}"}}',
        "pcie_rx_bytes": f'DCGM_FI_DEV_PCIE_RX_BYTES{{node="{node_name}"}}',
        "thermal_violations": f'DCGM_FI_DEV_THERMAL_VIOLATION{{node="{node_name}"}}',
        "power_violations": f'DCGM_FI_DEV_POWER_VIOLATION{{node="{node_name}"}}',
        "nvlink_flit_errors": f'DCGM_FI_DEV_NVLINK_FLIT_ERRORS_TOTAL{{node="{node_name}"}}',
        "ecc_sbe_errors": f'DCGM_FI_DEV_ECC_SBE_VOL_TOTAL{{node="{node_name}"}}',
        "ecc_dbe_errors": f'DCGM_FI_DEV_ECC_DBE_VOL_TOTAL{{node="{node_name}"}}'
    }
    
    successful_metrics = 0
    
    try:
        logger.info(f"Collecting DCGM metrics for node '{node_name}' from {start_time.isoformat()} to {end_time.isoformat()}")
        
        for metric_name, query in dcgm_queries.items():
            try:
                # Get current values
                current_result = prom_api_client.custom_query(query=query)
                
                # Get range data for trend analysis
                range_result = prom_api_client.custom_query_range(
                    query=query,
                    start_time=start_time,
                    end_time=end_time,
                    step='1m'
                )
                
                metric_data = {
                    "current_value": None,
                    "max_value": None,
                    "min_value": None,
                    "avg_value": None,
                    "data_points": 0,
                    "trend": "stable"
                }
                
                # Process current value
                if current_result:
                    metric_data["current_value"] = float(current_result[0]['value'][1])
                    successful_metrics += 1
                
                # Process range data for statistics
                if range_result:
                    values = []
                    for series in range_result:
                        for timestamp, value in series['values']:
                            try:
                                values.append(float(value))
                            except (ValueError, TypeError):
                                continue
                    
                    if values:
                        metric_data["data_points"] = len(values)
                        metric_data["max_value"] = max(values)
                        metric_data["min_value"] = min(values)
                        metric_data["avg_value"] = sum(values) / len(values)
                        
                        # Simple trend analysis
                        if len(values) >= 3:
                            first_third = values[:len(values)//3]
                            last_third = values[-len(values)//3:]
                            first_avg = sum(first_third) / len(first_third)
                            last_avg = sum(last_third) / len(last_third)
                            
                            if last_avg > first_avg * 1.1:
                                metric_data["trend"] = "increasing"
                            elif last_avg < first_avg * 0.9:
                                metric_data["trend"] = "decreasing"
                
                dcgm_data["metrics"][metric_name] = metric_data
                logger.debug(f"Collected {metric_name}: {metric_data}")
                
            except Exception as e:
                logger.warning(f"Failed to collect DCGM metric '{metric_name}': {e}")
                dcgm_data["metrics"][metric_name] = {
                    "error": str(e),
                    "current_value": None
                }
        
        # Analyze for anomalies
        dcgm_data["anomalies"] = analyze_dcgm_anomalies(dcgm_data["metrics"])
        dcgm_data["summary"] = generate_dcgm_summary(dcgm_data["metrics"], dcgm_data["anomalies"])
        
        logger.info(f"DCGM metrics collection completed for node '{node_name}'. Found {len(dcgm_data['anomalies'])} anomalies.")
        
        # Record metrics
        if "dcgm_metrics_collected" in otel_metrics_instruments:
            otel_metrics_instruments["dcgm_metrics_collected"].add(successful_metrics, {
                "node_name": node_name
            })
        
        if "dcgm_anomalies_detected" in otel_metrics_instruments:
            otel_metrics_instruments["dcgm_anomalies_detected"].add(len(dcgm_data["anomalies"]), {
                "node_name": node_name
            })
        
    except Exception as e:
        logger.error(f"Failed to collect DCGM metrics for node '{node_name}': {e}")
        dcgm_data["metrics_available"] = False
        dcgm_data["error"] = str(e)
    
    finally:
        dcgm_duration = time.time() - dcgm_start_time
        if "dcgm_collection_duration" in otel_metrics_instruments:
            otel_metrics_instruments["dcgm_collection_duration"].record(dcgm_duration, {
                "node_name": node_name,
                "success": str(dcgm_data["metrics_available"]).lower()
            })
    
    return dcgm_data


def analyze_dcgm_anomalies(metrics):
    """
    Analyze DCGM metrics for anomalies based on predefined thresholds.
    Returns a list of detected anomalies.
    """
    anomalies = []
    
    # GPU Temperature Analysis
    if "gpu_temperature" in metrics and metrics["gpu_temperature"].get("current_value") is not None:
        temp = metrics["gpu_temperature"]["current_value"]
        max_temp = metrics["gpu_temperature"].get("max_value")
        
        if temp >= DCGM_ANOMALY_THRESHOLDS['gpu_temp_critical']:
            anomalies.append({
                "type": "critical_temperature",
                "severity": "critical",
                "message": f"🔥 CRITICAL: GPU temperature at {temp:.1f}°C (threshold: {DCGM_ANOMALY_THRESHOLDS['gpu_temp_critical']}°C)",
                "current_value": temp,
                "threshold": DCGM_ANOMALY_THRESHOLDS['gpu_temp_critical']
            })
        elif temp >= DCGM_ANOMALY_THRESHOLDS['gpu_temp_high']:
            anomalies.append({
                "type": "high_temperature",
                "severity": "high",
                "message": f"🌡️ HIGH: GPU temperature at {temp:.1f}°C (threshold: {DCGM_ANOMALY_THRESHOLDS['gpu_temp_high']}°C)",
                "current_value": temp,
                "threshold": DCGM_ANOMALY_THRESHOLDS['gpu_temp_high']
            })
        
        # Only check for temperature spikes if max_temp is available and valid
        if max_temp is not None and max_temp > temp and max_temp >= DCGM_ANOMALY_THRESHOLDS['gpu_temp_high']:
            anomalies.append({
                "type": "temperature_spike",
                "severity": "medium",
                "message": f"📈 Temperature spike detected: peaked at {max_temp:.1f}°C (current: {temp:.1f}°C)",
                "current_value": temp,
                "max_value": max_temp
            })
    
    # Memory Utilization Analysis
    if "memory_utilization" in metrics and metrics["memory_utilization"].get("current_value") is not None:
        mem_util = metrics["memory_utilization"]["current_value"]
        
        if mem_util >= DCGM_ANOMALY_THRESHOLDS['mem_util_high']:
            anomalies.append({
                "type": "high_memory_utilization",
                "severity": "medium",
                "message": f"💾 HIGH: GPU memory utilization at {mem_util:.1f}% (threshold: {DCGM_ANOMALY_THRESHOLDS['mem_util_high']}%)",
                "current_value": mem_util,
                "threshold": DCGM_ANOMALY_THRESHOLDS['mem_util_high']
            })
    
    # NVLink Flit Errors Analysis
    if "nvlink_flit_errors" in metrics and metrics["nvlink_flit_errors"].get("current_value") is not None:
        flit_errors = metrics["nvlink_flit_errors"]["current_value"]
        
        if flit_errors >= DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_critical']:
            anomalies.append({
                "type": "critical_nvlink_flit_errors",
                "severity": "critical",
                "message": f"🚨 CRITICAL: NVLink flit errors at {int(flit_errors)} (threshold: ≥{DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_critical']})",
                "current_value": flit_errors,
                "threshold": DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_critical']
            })
        elif flit_errors >= DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_high']:
            anomalies.append({
                "type": "high_nvlink_flit_errors",
                "severity": "high",
                "message": f"⚠️ HIGH: NVLink flit errors at {int(flit_errors)} (threshold: ≥{DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_high']})",
                "current_value": flit_errors,
                "threshold": DCGM_ANOMALY_THRESHOLDS['nvlink_flit_errors_high']
            })
    
    # ECC Single-Bit Errors Analysis
    if "ecc_sbe_errors" in metrics and metrics["ecc_sbe_errors"].get("current_value") is not None:
        sbe_errors = metrics["ecc_sbe_errors"]["current_value"]
        
        if sbe_errors >= DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_critical']:
            anomalies.append({
                "type": "critical_ecc_sbe_errors",
                "severity": "critical",
                "message": f"🚨 CRITICAL: ECC single-bit errors at {int(sbe_errors)} (threshold: ≥{DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_critical']})",
                "current_value": sbe_errors,
                "threshold": DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_critical']
            })
        elif sbe_errors >= DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_high']:
            anomalies.append({
                "type": "high_ecc_sbe_errors",
                "severity": "high",
                "message": f"⚠️ HIGH: ECC single-bit errors at {int(sbe_errors)} (threshold: ≥{DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_high']})",
                "current_value": sbe_errors,
                "threshold": DCGM_ANOMALY_THRESHOLDS['ecc_sbe_errors_high']
            })
    
    # ECC Double-Bit Errors Analysis - any DBE is critical
    if "ecc_dbe_errors" in metrics and metrics["ecc_dbe_errors"].get("current_value") is not None:
        dbe_errors = metrics["ecc_dbe_errors"]["current_value"]
        
        if dbe_errors >= DCGM_ANOMALY_THRESHOLDS['ecc_dbe_errors_count']:
            anomalies.append({
                "type": "critical_ecc_dbe_errors",
                "severity": "critical",
                "message": f"🚨 CRITICAL: ECC double-bit errors detected: {int(dbe_errors)} (any DBE is critical)",
                "current_value": dbe_errors,
                "threshold": DCGM_ANOMALY_THRESHOLDS['ecc_dbe_errors_count']
            })
    
    # PCIe Traffic Analysis
    pcie_tx = metrics.get("pcie_tx_bytes", {}).get("current_value")
    pcie_rx = metrics.get("pcie_rx_bytes", {}).get("current_value")
    
    if pcie_tx is not None and pcie_rx is not None:
        total_pcie_traffic = (pcie_tx + pcie_rx) / (1024**3)  # Convert to GB/s
        
        if total_pcie_traffic > DCGM_ANOMALY_THRESHOLDS['pcie_traffic_high']:
            anomalies.append({
                "type": "high_pcie_traffic",
                "severity": "medium",
                "message": f"🚌 HIGH: PCIe traffic at {total_pcie_traffic:.2f} GB/s (TX: {pcie_tx/(1024**3):.2f}, RX: {pcie_rx/(1024**3):.2f})",
                "current_value": total_pcie_traffic,
                "threshold": DCGM_ANOMALY_THRESHOLDS['pcie_traffic_high']
            })
    
    # Thermal Violations Analysis
    if "thermal_violations" in metrics and metrics["thermal_violations"].get("current_value") is not None:
        thermal_violations = metrics["thermal_violations"]["current_value"]
        
        if thermal_violations >= DCGM_ANOMALY_THRESHOLDS['thermal_violation_count']:
            anomalies.append({
                "type": "thermal_violations",
                "severity": "high",
                "message": f"🚨 THERMAL: {int(thermal_violations)} thermal violation(s) detected",
                "current_value": thermal_violations,
                "threshold": DCGM_ANOMALY_THRESHOLDS['thermal_violation_count']
            })
    
    # Power Violations Analysis
    if "power_violations" in metrics and metrics["power_violations"].get("current_value") is not None:
        power_violations = metrics["power_violations"]["current_value"]
        
        if power_violations >= DCGM_ANOMALY_THRESHOLDS['power_violation_count']:
            anomalies.append({
                "type": "power_violations",
                "severity": "high",
                "message": f"⚡ POWER: {int(power_violations)} power violation(s) detected",
                "current_value": power_violations,
                "threshold": DCGM_ANOMALY_THRESHOLDS['power_violation_count']
            })
    
    return anomalies


def generate_dcgm_summary(metrics, anomalies):
    """
    Generate a human-readable summary of DCGM metrics and anomalies.
    """
    if not metrics:
        return "No DCGM metrics available"
    
    summary_parts = []
    
    # Temperature summary
    if "gpu_temperature" in metrics and metrics["gpu_temperature"].get("current_value") is not None:
        temp = metrics["gpu_temperature"]["current_value"]
        max_temp = metrics["gpu_temperature"].get("max_value")
        if max_temp is not None:
            summary_parts.append(f"GPU Temp: {temp:.1f}°C (max: {max_temp:.1f}°C)")
        else:
            summary_parts.append(f"GPU Temp: {temp:.1f}°C")
    
    # Memory utilization summary
    if "memory_utilization" in metrics and metrics["memory_utilization"].get("current_value") is not None:
        mem_util = metrics["memory_utilization"]["current_value"]
        summary_parts.append(f"Memory Util: {mem_util:.1f}%")
    
    # PCIe traffic summary
    pcie_tx = metrics.get("pcie_tx_bytes", {}).get("current_value")
    pcie_rx = metrics.get("pcie_rx_bytes", {}).get("current_value")
    if pcie_tx is not None and pcie_rx is not None:
        total_pcie = (pcie_tx + pcie_rx) / (1024**3)
        summary_parts.append(f"PCIe: {total_pcie:.2f} GB/s")
    
    # ECC Errors summary
    sbe_errors = metrics.get("ecc_sbe_errors", {}).get("current_value")
    dbe_errors = metrics.get("ecc_dbe_errors", {}).get("current_value")
    if (sbe_errors is not None and sbe_errors > 0) or (dbe_errors is not None and dbe_errors > 0):
        sbe_count = int(sbe_errors) if sbe_errors is not None else 0
        dbe_count = int(dbe_errors) if dbe_errors is not None else 0
        summary_parts.append(f"ECC Errors: {sbe_count} SBE, {dbe_count} DBE")
    
    # NVLink Flit Errors summary
    flit_errors = metrics.get("nvlink_flit_errors", {}).get("current_value")
    if flit_errors is not None and flit_errors > 0:
        summary_parts.append(f"NVLink Flit Errors: {int(flit_errors)}")
    
    # Violations summary
    thermal_violations = metrics.get("thermal_violations", {}).get("current_value")
    power_violations = metrics.get("power_violations", {}).get("current_value")
    if (thermal_violations is not None and thermal_violations > 0) or (power_violations is not None and power_violations > 0):
        thermal_count = int(thermal_violations) if thermal_violations is not None else 0
        power_count = int(power_violations) if power_violations is not None else 0
        summary_parts.append(f"Violations: {thermal_count} thermal, {power_count} power")
    
    base_summary = " | ".join(summary_parts) if summary_parts else "No metrics data"
    
    # Add anomaly count
    if anomalies:
        critical_count = len([a for a in anomalies if a.get("severity") == "critical"])
        high_count = len([a for a in anomalies if a.get("severity") == "high"])
        medium_count = len([a for a in anomalies if a.get("severity") == "medium"])
        
        anomaly_summary = f" | 🚨 {len(anomalies)} anomalies"
        if critical_count > 0:
            anomaly_summary += f" ({critical_count} critical"
            if high_count > 0 or medium_count > 0:
                anomaly_summary += f", {high_count} high, {medium_count} medium"
            anomaly_summary += ")"
        elif high_count > 0:
            anomaly_summary += f" ({high_count} high"
            if medium_count > 0:
                anomaly_summary += f", {medium_count} medium"
            anomaly_summary += ")"
        elif medium_count > 0:
            anomaly_summary += f" ({medium_count} medium)"
        
        return base_summary + anomaly_summary
    else:
        return base_summary + " | ✅ No anomalies detected"

# --- Main Execution ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SRE XID RCA Service")
    parser.add_argument('--prometheus-url', help='Prometheus server URL for context queries (overrides PROMETHEUS_URL env var)')
    parser.add_argument('--port', type=int, default=5001, help='Port for the webhook listener')
    parser.add_argument('--openai-api-key', help='OpenAI API Key (overrides OPENAI_API_KEY env var or hardcoded value)')
    args = parser.parse_args()

    if args.openai_api_key:
        OPENAI_API_KEY = args.openai_api_key
        logger.info("Using OpenAI API Key from command line argument.")

    if not SLACK_WEBHOOK_URL:
        logger.error("FATAL: SLACK_WEBHOOK_URL environment variable not set.")
        sys.exit(1)
    
    if not OPENAI_API_KEY:
         logger.warning("OpenAI API Key is not configured. RCA processing will fail without valid API key.")

    try:
        initialize_clients(prometheus_url_arg=args.prometheus_url)
        logger.info("Kubernetes and Prometheus API clients initialized.")
    except RcaServiceError as e:
        logger.error(f"FATAL: Could not start service due to client init failure: {e}")
        sys.exit(1)
    except Exception as e_main_init:
        logger.error(f"FATAL: Unexpected error during initial client setup: {e_main_init}")
        sys.exit(1)

    # Initialize the queue and worker threads
    try:
        initialize_queue()
        logger.info("Alert processing queue initialized.")
    except Exception as e:
        logger.error(f"FATAL: Could not initialize queue: {e}")
        sys.exit(1)

    logger.info(f"Starting SRE XID RCA service on port {args.port}")
    
    try:
        app.run(host='0.0.0.0', port=args.port, debug=False)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        # Graceful shutdown
        shutdown_queue()
