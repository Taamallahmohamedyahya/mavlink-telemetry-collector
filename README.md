# MAVLink Telemetry Collector

A Python-based MAVLink UDP collector for telemetry monitoring.

## Features
- MAVLink 2 support
- UDP packet collection
- Rolling traffic metrics
- Heartbeat monitoring
- GPS and position tracking
- EKF status monitoring
- CSV logging
- Live JSON export for dashboards

## Requirements
- Python 3.9+
- pymavlink

## Installation
```bash
git clone https://github.com/Taamallahmohamedyahya/mavlink-telemetry-collector.git
cd mavlink-telemetry-collector
pip install -r requirements.txt
```


## Usage

To use this collector, you need to connect your PC to the SkyDroid controller and configure QGroundControl correctly.

### 1. Connect to SkyDroid
Share WiFi or Ethernet from your SkyDroid controller and connect your Windows PC to it.

### 2. Get your IP address on Windows
- Open **Command Prompt**
- Run:
  ```bash
  ipconfig
  ```

## ⚠️ Dashboard Access

The dashboard is not included in this repository.  
If you need access, please contact me at: taamallahyahya7@gmail.com
