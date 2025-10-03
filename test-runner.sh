#!/bin/bash

# Test Runner for My Portfolio E2E Tests
# This script manages the Flask server and runs Playwright tests

set -e  # Exit on error

# Configuration
PORT=5001
SERVER_PID_FILE="/tmp/portfolio-server.pid"
SERVER_LOG_FILE="/tmp/portfolio-server.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if server is running
is_server_running() {
    if [ -f "$SERVER_PID_FILE" ]; then
        local pid=$(cat "$SERVER_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        fi
    fi
    return 1
}

# Function to start the Flask server
start_server() {
    print_info "Starting Flask server on port $PORT..."
    
    if is_server_running; then
        print_warning "Server is already running (PID: $(cat $SERVER_PID_FILE))"
        return 0
    fi
    
    # Check if port is already in use
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        print_error "Port $PORT is already in use by another process"
        lsof -Pi :$PORT -sTCP:LISTEN
        return 1
    fi
    
    # Start Flask server in background
    export FLASK_PORT=$PORT
    nohup python app.py > "$SERVER_LOG_FILE" 2>&1 &
    local pid=$!
    echo $pid > "$SERVER_PID_FILE"
    
    # Wait for server to be ready
    print_info "Waiting for server to be ready..."
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "http://127.0.0.1:$PORT/" > /dev/null 2>&1; then
            print_success "Server started successfully (PID: $pid)"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    
    print_error "Server failed to start within timeout"
    cat "$SERVER_LOG_FILE"
    return 1
}

# Function to stop the Flask server
stop_server() {
    print_info "Stopping Flask server..."
    
    if [ -f "$SERVER_PID_FILE" ]; then
        local pid=$(cat "$SERVER_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid"
            sleep 2
            
            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                print_warning "Force killing server..."
                kill -9 "$pid"
            fi
            
            print_success "Server stopped"
        else
            print_warning "Server process not found (PID: $pid)"
        fi
        rm -f "$SERVER_PID_FILE"
    else
        print_warning "Server PID file not found"
    fi
    
    # Clean up any remaining processes on the port
    local remaining_pid=$(lsof -ti:$PORT)
    if [ -n "$remaining_pid" ]; then
        print_warning "Cleaning up remaining process on port $PORT (PID: $remaining_pid)"
        kill -9 "$remaining_pid" 2>/dev/null || true
    fi
}

# Function to install dependencies
install_deps() {
    print_info "Installing dependencies..."
    
    # Install Python dependencies if requirements.txt exists
    if [ -f "requirements.txt" ]; then
        print_info "Installing Python dependencies..."
        pip install -q -r requirements.txt
    fi
    
    # Install Node.js dependencies
    print_info "Installing Node.js dependencies..."
    npm install
    
    # Install Playwright browsers
    print_info "Installing Playwright browsers..."
    npx playwright install chromium webkit
    npx playwright install-deps
    
    print_success "Dependencies installed successfully"
}

# Function to run all tests
run_all_tests() {
    local headed_flag=""
    if [ "$1" == "--headed" ]; then
        headed_flag="--headed"
        print_info "Running all tests in headed mode..."
    else
        print_info "Running all tests..."
    fi
    
    npx playwright test $headed_flag
}

# Function to run specific test file
run_test_file() {
    local test_file=$1
    local headed_flag=""
    
    if [ "$2" == "--headed" ]; then
        headed_flag="--headed"
        print_info "Running test file '$test_file' in headed mode..."
    else
        print_info "Running test file '$test_file'..."
    fi
    
    npx playwright test "$test_file" $headed_flag
}

# Function to show test report
show_report() {
    print_info "Opening test report..."
    npx playwright show-report
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start           Start the Flask server on port $PORT
    stop            Stop the Flask server
    install         Install all dependencies (Python, Node.js, Playwright browsers)
    test-all        Run all E2E tests
    test-filters    Run filter tests only
    test-data       Run data validation tests only
    test-export     Run export function tests only
    test FILE       Run specific test file
    report          Show test report
    help            Show this help message

Options:
    --headed        Run tests in headed mode (visible browser)

Examples:
    $0 install                      # Install all dependencies
    $0 start                        # Start server
    $0 test-all                     # Run all tests
    $0 test-all --headed            # Run all tests with visible browser
    $0 test-filters                 # Run only filter tests
    $0 test tests/filters.spec.js   # Run specific test file
    $0 test tests/filters.spec.js --headed  # Run specific file with visible browser
    $0 report                       # Show test report
    $0 stop                         # Stop server

Full workflow:
    $0 install && $0 start && $0 test-all && $0 report && $0 stop
EOF
}

# Main script logic
main() {
    local command=${1:-help}
    
    case "$command" in
        start)
            start_server
            ;;
        stop)
            stop_server
            ;;
        install)
            install_deps
            ;;
        test-all)
            run_all_tests "$2"
            ;;
        test-filters)
            run_test_file "tests/filters.spec.js" "$2"
            ;;
        test-data)
            run_test_file "tests/data.spec.js" "$2"
            ;;
        test-export)
            run_test_file "tests/export.spec.js" "$2"
            ;;
        test)
            if [ -z "$2" ]; then
                print_error "Please specify a test file"
                echo ""
                show_usage
                exit 1
            fi
            run_test_file "$2" "$3"
            ;;
        report)
            show_report
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            print_error "Unknown command: $command"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Trap to ensure server is stopped on script exit
trap 'if [ "$command" == "test-all" ] || [ "$command" == "test" ]; then stop_server; fi' EXIT

# Run main function
main "$@"
