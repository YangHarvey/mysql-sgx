import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent
import socketserver
import socket
import threading
import json
import signal
import sys
import os
import logging
import struct
import datetime
import decimal

# --- Configuration ---
MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "replicator",
    "passwd": "password"
}
REPLICATION_SERVER_ID = 101 # Unique ID for this script
FORWARDER_HOST = '0.0.0.0'  # Listen on all available network interfaces
FORWARDER_PORT = 9991       # Port for clients to connect to
BINLOG_STATE_FILE = 'binlog_forwarder_state.json'

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Forwarder")

# --- Global State ---
binlog_stream = None
server = None
connected_clients = set()
clients_lock = threading.Lock()
last_log_file = None
last_log_pos = None
# last_gtid_set = None # If using GTID

# --- State Management ---
def load_binlog_state():
    """Load binlog position from state file."""
    global last_log_file, last_log_pos #, last_gtid_set
    if os.path.exists(BINLOG_STATE_FILE):
        try:
            with open(BINLOG_STATE_FILE, 'r') as f:
                state = json.load(f)
                last_log_file = state.get('log_file')
                last_log_pos = state.get('log_pos')
                # last_gtid_set = state.get('gtid_set') # If using GTID
                logger.info(f"Loaded binlog state: file={last_log_file}, pos={last_log_pos}") #, gtid={last_gtid_set}")
        except Exception as e:
            logger.error(f"Error loading binlog state: {e}. Starting fresh.")
            # Reset state if loading fails
            last_log_file = None
            last_log_pos = None
            # last_gtid_set = None
    else:
        logger.info("No binlog state file found. Starting fresh.")

def save_binlog_state(log_file, log_pos): #, gtid_set=None):
    """Save current binlog position to state file."""
    state = {'log_file': log_file, 'log_pos': log_pos} #, 'gtid_set': gtid_set}
    try:
        with open(BINLOG_STATE_FILE, 'w') as f:
            json.dump(state, f)
        # logger.debug(f"Saved binlog state: file={log_file}, pos={log_pos}") #, gtid={gtid_set}")
    except Exception as e:
        logger.error(f"Error saving binlog state: {e}")

# --- Event Formatting ---
def default_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    elif isinstance(obj, datetime.timedelta):
        return str(obj)
    elif isinstance(obj, bytes):
         # Attempt to decode bytes, or represent as string
         try:
            return obj.decode('utf-8', errors='replace')
         except Exception:
            return repr(obj) # Fallback to representation
    elif isinstance(obj, decimal.Decimal): # **** 添加对 Decimal 的显式处理 ****
        return str(obj)
    # Add other types if needed
    # raise TypeError(f"Type {type(obj)} not serializable")
    logger.warning(f"Type {type(obj)} not directly serializable, converting to string: {str(obj)}")
    return str(obj) # Fallback: convert unknown types to string


def format_event_to_json(event):
    """Converts a BinLog event into a JSON serializable dictionary."""
    event_data = {
        "event_type": type(event).__name__,
        "timestamp": event.timestamp, # Unix timestamp
        "log_file": binlog_stream.log_file if binlog_stream else "N/A",
        "log_pos": binlog_stream.log_pos if binlog_stream else "N/A",
        # GTID info can be added here if available/needed
    }
    if hasattr(event, 'schema'):
        event_data["schema"] = event.schema
    if hasattr(event, 'table'):
        event_data["table"] = event.table

    if isinstance(event, WriteRowsEvent):
        event_data["rows"] = [{"values": row["values"]} for row in event.rows]
    elif isinstance(event, UpdateRowsEvent):
        event_data["rows"] = [{"before_values": row["before_values"], "after_values": row["after_values"]} for row in event.rows]
    elif isinstance(event, DeleteRowsEvent):
        event_data["rows"] = [{"values": row["values"]} for row in event.rows]
    elif isinstance(event, QueryEvent):
        event_data["query"] = event.query
        event_data["schema"] = event.schema
        event_data["execution_time"] = event.execution_time
        # event_data["error_code"] = event.error_code # 0 if successful
    # Add more event types if needed (e.g., QueryEvent for DDL)
    # elif isinstance(event, QueryEvent):
    #     event_data["query"] = event.query
    #     event_data["schema"] = event.schema

    try:
        return json.dumps(event_data, default=default_serializer)
    except TypeError as e:
        logger.error(f"JSON serialization error: {e} for event: {event_data}")
        # Fallback or skip event? Return None to indicate failure.
        return None


# --- Network Handling ---
class ClientHandler(socketserver.BaseRequestHandler):
    """Handles connections from clients."""
    def setup(self):
        logger.info(f"Client connected: {self.client_address}")
        with clients_lock:
            connected_clients.add(self)

    def handle(self):
        # Keep connection alive, but handle is mainly for receiving
        # Sending is done by the main Binlog reading thread
        # We can add a simple loop here to detect disconnection
        try:
            while True:
                # Keep the handler "busy" or wait for a signal
                # A simple way is to just sleep, the main thread does the sending
                # Alternatively, could implement a ping/pong here
                data = self.request.recv(1024) # Try reading to detect disconnect
                if not data:
                    break # Client disconnected cleanly
                # If data is received, it might be unexpected unless a protocol is defined
                # logger.debug(f"Received unexpected data from {self.client_address}: {data}")
        except ConnectionResetError:
             logger.warning(f"Client {self.client_address} forcibly closed connection.")
        except Exception as e:
            logger.error(f"Error in client handler {self.client_address}: {e}")
        # finally:
        #     # Cleanup happens in finish()

    def finish(self):
        logger.info(f"Client disconnected: {self.client_address}")
        with clients_lock:
            connected_clients.discard(self)

    def send_event(self, json_event_bytes):
        """Sends a JSON event (as bytes) to this client with length prefix."""
        try:
            # 1. Pack length as 4-byte unsigned integer (network byte order)
            msg_len = len(json_event_bytes)
            len_prefix = struct.pack('>I', msg_len)

            # 2. Send length prefix
            self.request.sendall(len_prefix)

            # 3. Send actual message
            self.request.sendall(json_event_bytes)
            # logger.debug(f"Sent {msg_len} bytes to {self.client_address}")
            return True
        except (socket.error, BrokenPipeError, ConnectionResetError) as e:
            logger.warning(f"Failed to send to client {self.client_address}: {e}. Removing client.")
            # Schedule removal from the main thread or handle removal carefully here
            # For simplicity, we'll let finish() handle removal upon error detection
            # But we need to signal that sending failed
            with clients_lock:
                 connected_clients.discard(self) # Remove immediately on send failure
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to {self.client_address}: {e}")
            with clients_lock:
                 connected_clients.discard(self)
            return False

def broadcast_event(json_event_str):
    """Formats and sends an event to all connected clients."""
    if not json_event_str:
        logger.warning("Skipping broadcast of unserializable event.")
        return

    json_event_bytes = json_event_str.encode('utf-8')
    failed_clients = set()

    # Iterate over a copy of the set in case it's modified during iteration
    with clients_lock:
        current_clients = set(connected_clients) # Make a copy for safe iteration

    if not current_clients:
        # logger.debug("No clients connected, skipping broadcast.")
        return

    logger.info(f"Broadcasting event to {len(current_clients)} client(s)...")
    for client_handler in current_clients:
        if not client_handler.send_event(json_event_bytes):
             # send_event handles removing the client from connected_clients on failure
             pass # Removal now handled inside send_event


# --- Main Binlog Reading Loop ---
def run_binlog_listener():
    global binlog_stream
    load_binlog_state()

    stream_params = {
        "connection_settings": MYSQL_SETTINGS,
        "server_id": REPLICATION_SERVER_ID,
        "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent], # Add QueryEvent if needed
        "blocking": True,
        "resume_stream": True if last_log_file and last_log_pos else False,
        "log_file": last_log_file,
        "log_pos": last_log_pos,
        # "auto_position": last_gtid_set, # If using GTID
        "slave_heartbeat": 10, # Keep connection alive
    }

    logger.info("Starting Binlog stream reader...")
    try:
        binlog_stream = BinLogStreamReader(**stream_params)

        for event in binlog_stream:
            logger.debug(f"Received binlog event: {type(event).__name__}")
            json_event_str = format_event_to_json(event)
            if json_event_str:
                broadcast_event(json_event_str)
                # Save state *after* attempting to broadcast
                save_binlog_state(binlog_stream.log_file, binlog_stream.log_pos) #, binlog_stream.gtid_set)

    except pymysql.err.OperationalError as e:
         logger.error(f"MySQL connection error: {e}")
         # Handle error, maybe attempt reconnect after a delay? Needs complex logic.
         # For simplicity, we exit here.
         stop_server() # Signal the server to stop
    except Exception as e:
        logger.error(f"Error in Binlog stream reader: {e}", exc_info=True)
        stop_server() # Signal the server to stop
    finally:
        if binlog_stream:
            binlog_stream.close()
            logger.info("Binlog stream closed.")


# --- Server Start/Stop ---
def start_server():
    global server
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer((FORWARDER_HOST, FORWARDER_PORT), ClientHandler)
    logger.info(f"Forwarder service starting on {FORWARDER_HOST}:{FORWARDER_PORT}")
    server.serve_forever() # This blocks until shutdown() is called

def stop_server(sig=None, frame=None):
    global server
    logger.info("Shutting down server...")
    if server:
        # Shutdown the server (stops accepting new connections)
        server.shutdown()
        # Close the server socket
        server.server_close()
        logger.info("Server stopped.")
    # The binlog stream loop should also exit gracefully if running in a separate thread
    # If binlog loop is main thread, sys.exit() might be needed after cleanup
    if binlog_stream:
         binlog_stream.close() # Ensure binlog stream is closed
         logger.info("Binlog stream explicitly closed during shutdown.")
    sys.exit(0)


# --- Main Execution ---
if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    # Start the Binlog listener in a separate thread
    # So the main thread can run the socket server
    binlog_thread = threading.Thread(target=run_binlog_listener, daemon=True) # Daemon so it exits if main thread exits
    binlog_thread.start()

    # Start the socket server in the main thread (blocks)
    try:
         start_server()
    except Exception as e:
         logger.error(f"Failed to start server: {e}")
         stop_server()