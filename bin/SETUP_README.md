# Glow Linux Setup Script

Automated setup script for installing all Glow build requirements on Linux systems.

## Supported Distributions

- **Ubuntu / Debian** (apt-based)
- **CentOS / RHEL / Fedora** (yum-based)
- **Arch / Manjaro** (pacman-based)

## What It Installs

The script checks for and optionally installs:

1. **Java 8** (OpenJDK)
   - Required for building Glow with Spark 3.x
   - Detects existing Java installations

2. **sbt 1.10.4** (Scala Build Tool)
   - Required for building Scala artifacts
   - Adds official sbt repository

3. **Git**
   - Required for version control

4. **Miniconda**
   - Python environment manager
   - Installs to `~/miniconda3`

5. **Glow Conda Environment**
   - Creates or updates the `glow` conda environment
   - Installs all Python dependencies from `python/environment.yml`

## Usage

### Interactive Mode (Recommended)

Run the script and it will prompt you for each component:

```bash
./bin/setup-linux.sh
```

The script will:
- Check if each requirement is already installed
- Ask permission before installing missing components
- Verify the installation at the end

### What to Expect

```
[INFO] ========================================
[INFO] Glow Build Environment Setup
[INFO] ========================================

[INFO] Detected distribution: ubuntu
[INFO] Checking Java installation...
[WARN] Java not found
Install Java 8? (y/n) y
[INFO] Installing Java 8...
[SUCCESS] Java installed successfully

[INFO] Checking Git installation...
[SUCCESS] Git found: version 2.34.1

... (continues for all components)
```

## Post-Installation

After running the script:

### 1. Restart Your Shell (if Conda was installed)
```bash
# Close and reopen your terminal, or:
source ~/.bashrc
```

### 2. Activate the Glow Environment
```bash
conda activate glow
```

### 3. Build Glow Artifacts
```bash
# Build both Scala JAR and Python wheel
bin/build --scala --python

# Or build individually
bin/build --scala   # Just the JAR
bin/build --python  # Just the wheel
```

### 4. Verify with sbt
```bash
# Compile the code
sbt compile

# Run tests
sbt core/test
sbt python/test
```

## Manual Installation

If the script doesn't work for your distribution, install manually:

### Java 8
```bash
# Ubuntu/Debian
sudo apt-get install openjdk-8-jdk

# CentOS/RHEL/Fedora
sudo yum install java-1.8.0-openjdk-devel

# Or download from: https://adoptium.net/
```

### sbt
Follow instructions at: https://www.scala-sbt.org/download.html

### Git
```bash
# Ubuntu/Debian
sudo apt-get install git

# CentOS/RHEL/Fedora
sudo yum install git
```

### Miniconda
```bash
# Download and install
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda3
~/miniconda3/bin/conda init bash
```

### Glow Environment
```bash
conda env create -f python/environment.yml
conda activate glow
```

## Troubleshooting

### Script Fails with Permission Denied
Make sure the script is executable:
```bash
chmod +x bin/setup-linux.sh
```

### Conda Command Not Found After Installation
Restart your shell or run:
```bash
source ~/.bashrc
# or
exec bash
```

### Java Version Issues
Check your Java version:
```bash
java -version
```

If you have multiple Java versions, set `JAVA_HOME`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

### sbt Installation Fails
Try manual installation:
1. Download from: https://www.scala-sbt.org/download.html
2. Extract and add to PATH

### Conda Environment Issues
Remove and recreate:
```bash
conda env remove -n glow
conda env create -f python/environment.yml
```

## Script Features

- ✅ **Distribution Detection** - Automatically detects Ubuntu, Debian, CentOS, RHEL, Fedora, Arch, Manjaro
- ✅ **Smart Checking** - Skips already-installed components
- ✅ **Interactive Prompts** - Asks before installing each component
- ✅ **Colored Output** - Easy to read status messages
- ✅ **Error Handling** - Exits on errors with clear messages
- ✅ **Verification** - Confirms all components are properly installed
- ✅ **Architecture Support** - Works on x86_64 and aarch64 (ARM)

## Requirements

- Linux system (Ubuntu, Debian, CentOS, RHEL, Fedora, Arch, or Manjaro)
- `sudo` access (for installing system packages)
- Internet connection (for downloading packages)
- Bash shell

## Security Notes

- The script requires `sudo` for installing system packages
- **DO NOT** run the script as root (it will exit with an error)
- Review the script before running if you have security concerns
- Official package repositories are used for all installations

## Environment Variables

The script respects the following:

- `CONDA_PREFIX` - Existing conda installation
- Standard package manager environment variables

## Next Steps

After setup, see:
- [BUILD_REQUIREMENTS.md](../BUILD_REQUIREMENTS.md) - Detailed requirements documentation
- [README.md](../README.md) - Main project documentation
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contributing guidelines

## Support

For issues or questions:
- Check existing issues: https://github.com/projectglow/glow/issues
- Review troubleshooting section above
- Consult BUILD_REQUIREMENTS.md for manual installation steps

