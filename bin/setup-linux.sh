#!/bin/bash
# Glow Build Environment Setup Script for Linux
# This script checks for required dependencies and installs them if missing

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
    log_error "Please do not run this script as root"
    exit 1
fi

# Determine Linux distribution
if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO=$ID
else
    log_error "Cannot determine Linux distribution"
    exit 1
fi

log_info "Detected distribution: $DISTRO"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Java version
check_java() {
    log_info "Checking Java installation..."
    
    if command_exists java; then
        JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
        JAVA_MAJOR=$(echo "$JAVA_VERSION" | cut -d'.' -f1)
        
        # Handle Java 8 vs newer versioning (1.8.x vs 11.x)
        if [[ "$JAVA_VERSION" == 1.8* ]]; then
            JAVA_MAJOR=8
        fi
        
        log_success "Java found: version $JAVA_VERSION"
        
        if [ "$JAVA_MAJOR" -eq 8 ] || [ "$JAVA_MAJOR" -ge 11 ]; then
            return 0
        else
            log_warn "Java version $JAVA_VERSION found, but Java 8 or 11+ recommended"
            return 1
        fi
    else
        log_warn "Java not found"
        return 1
    fi
}

# Install Java
install_java() {
    log_info "Installing Java 8..."
    
    case $DISTRO in
        ubuntu|debian)
            sudo apt-get update
            sudo apt-get install -y openjdk-8-jdk
            ;;
        centos|rhel|fedora)
            sudo yum install -y java-1.8.0-openjdk-devel
            ;;
        arch|manjaro)
            sudo pacman -S --noconfirm jdk8-openjdk
            ;;
        *)
            log_error "Unsupported distribution for automatic Java installation"
            log_info "Please install Java 8 manually from: https://adoptium.net/"
            return 1
            ;;
    esac
    
    log_success "Java installed successfully"
}

# Check and install sbt
check_sbt() {
    log_info "Checking sbt installation..."
    
    if command_exists sbt; then
        SBT_VERSION=$(sbt --version 2>&1 | grep "sbt version" | awk '{print $4}')
        log_success "sbt found: version $SBT_VERSION"
        return 0
    else
        log_warn "sbt not found"
        return 1
    fi
}

install_sbt() {
    log_info "Installing sbt..."
    
    case $DISTRO in
        ubuntu|debian)
            # Add sbt repository
            echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
            echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
            curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
            sudo apt-get update
            sudo apt-get install -y sbt
            ;;
        centos|rhel|fedora)
            # Remove old sbt repo if exists
            sudo rm -f /etc/yum.repos.d/sbt-rpm.repo
            # Add sbt repository
            curl -fsSL https://www.scala-sbt.org/sbt-rpm.repo | sudo tee /etc/yum.repos.d/sbt-rpm.repo
            sudo yum install -y sbt
            ;;
        arch|manjaro)
            sudo pacman -S --noconfirm sbt
            ;;
        *)
            log_error "Unsupported distribution for automatic sbt installation"
            log_info "Please install sbt manually from: https://www.scala-sbt.org/download.html"
            return 1
            ;;
    esac
    
    log_success "sbt installed successfully"
}

# Check and install Git
check_git() {
    log_info "Checking Git installation..."
    
    if command_exists git; then
        GIT_VERSION=$(git --version | awk '{print $3}')
        log_success "Git found: version $GIT_VERSION"
        return 0
    else
        log_warn "Git not found"
        return 1
    fi
}

install_git() {
    log_info "Installing Git..."
    
    case $DISTRO in
        ubuntu|debian)
            sudo apt-get update
            sudo apt-get install -y git
            ;;
        centos|rhel|fedora)
            sudo yum install -y git
            ;;
        arch|manjaro)
            sudo pacman -S --noconfirm git
            ;;
        *)
            log_error "Unsupported distribution for automatic Git installation"
            return 1
            ;;
    esac
    
    log_success "Git installed successfully"
}

# Check and install Conda
check_conda() {
    log_info "Checking Conda installation..."
    
    if command_exists conda; then
        CONDA_VERSION=$(conda --version | awk '{print $2}')
        log_success "Conda found: version $CONDA_VERSION"
        return 0
    else
        log_warn "Conda not found"
        return 1
    fi
}

install_conda() {
    log_info "Installing Miniconda..."
    
    # Determine architecture
    ARCH=$(uname -m)
    if [ "$ARCH" = "x86_64" ]; then
        CONDA_INSTALLER="Miniconda3-latest-Linux-x86_64.sh"
    elif [ "$ARCH" = "aarch64" ]; then
        CONDA_INSTALLER="Miniconda3-latest-Linux-aarch64.sh"
    else
        log_error "Unsupported architecture: $ARCH"
        return 1
    fi
    
    CONDA_URL="https://repo.anaconda.com/miniconda/$CONDA_INSTALLER"
    TEMP_DIR=$(mktemp -d)
    
    log_info "Downloading Miniconda from $CONDA_URL..."
    curl -fsSL "$CONDA_URL" -o "$TEMP_DIR/$CONDA_INSTALLER"
    
    log_info "Installing Miniconda to $HOME/miniconda3..."
    bash "$TEMP_DIR/$CONDA_INSTALLER" -b -p "$HOME/miniconda3"
    
    # Clean up
    rm -rf "$TEMP_DIR"
    
    # Initialize conda
    eval "$($HOME/miniconda3/bin/conda shell.bash hook)"
    conda init bash
    
    log_success "Miniconda installed successfully"
    log_info "Please restart your shell or run: source ~/.bashrc"
}

# Check and setup Glow conda environment
setup_glow_environment() {
    log_info "Setting up Glow conda environment..."
    
    # Initialize conda for this script
    if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
        . "$HOME/miniconda3/etc/profile.d/conda.sh"
    elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
        . "$HOME/anaconda3/etc/profile.d/conda.sh"
    else
        log_error "Cannot find conda initialization script"
        return 1
    fi
    
    # Get the project root directory
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
    ENV_FILE="$PROJECT_ROOT/python/environment.yml"
    
    if [ ! -f "$ENV_FILE" ]; then
        log_error "Environment file not found: $ENV_FILE"
        return 1
    fi
    
    # Check if glow environment exists
    if conda env list | grep -q "^glow "; then
        log_info "Glow environment already exists. Updating..."
        conda env update -n glow -f "$ENV_FILE" --prune
    else
        log_info "Creating Glow environment..."
        conda env create -f "$ENV_FILE"
    fi
    
    log_success "Glow conda environment is ready"
    log_info "Activate it with: conda activate glow"
}

# Verify installation
verify_installation() {
    log_info "Verifying installation..."
    
    local all_good=true
    
    # Check Java
    if check_java; then
        log_success "✓ Java is properly installed"
    else
        log_error "✗ Java verification failed"
        all_good=false
    fi
    
    # Check sbt
    if check_sbt; then
        log_success "✓ sbt is properly installed"
    else
        log_error "✗ sbt verification failed"
        all_good=false
    fi
    
    # Check Git
    if check_git; then
        log_success "✓ Git is properly installed"
    else
        log_error "✗ Git verification failed"
        all_good=false
    fi
    
    # Check Conda
    if check_conda; then
        log_success "✓ Conda is properly installed"
    else
        log_error "✗ Conda verification failed"
        all_good=false
    fi
    
    if [ "$all_good" = true ]; then
        log_success "All requirements are installed!"
        return 0
    else
        log_error "Some requirements failed verification"
        return 1
    fi
}

# Main installation flow
main() {
    echo ""
    log_info "========================================"
    log_info "Glow Build Environment Setup"
    log_info "========================================"
    echo ""
    
    # Check and install Java
    if ! check_java; then
        read -p "Install Java 8? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_java
        else
            log_warn "Skipping Java installation"
        fi
    fi
    
    # Check and install Git
    if ! check_git; then
        read -p "Install Git? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_git
        else
            log_warn "Skipping Git installation"
        fi
    fi
    
    # Check and install sbt
    if ! check_sbt; then
        read -p "Install sbt? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_sbt
        else
            log_warn "Skipping sbt installation"
        fi
    fi
    
    # Check and install Conda
    if ! check_conda; then
        read -p "Install Miniconda? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_conda
        else
            log_warn "Skipping Conda installation"
        fi
    fi
    
    # Setup Glow environment
    if check_conda; then
        read -p "Setup Glow conda environment? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            setup_glow_environment
        else
            log_warn "Skipping Glow environment setup"
        fi
    fi
    
    echo ""
    log_info "========================================"
    log_info "Verification"
    log_info "========================================"
    echo ""
    
    verify_installation
    
    echo ""
    log_info "========================================"
    log_info "Next Steps"
    log_info "========================================"
    echo ""
    log_info "1. If you just installed Conda, restart your shell or run:"
    log_info "   source ~/.bashrc"
    echo ""
    log_info "2. Activate the Glow environment:"
    log_info "   conda activate glow"
    echo ""
    log_info "3. Build Glow artifacts:"
    log_info "   bin/build --scala --python"
    echo ""
    log_info "4. Or use sbt directly:"
    log_info "   sbt compile"
    echo ""
    
    log_success "Setup complete!"
}

# Run main function
main "$@"

