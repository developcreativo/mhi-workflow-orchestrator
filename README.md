# MHI Workflow Orchestrator

<div align="center">

![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.9+-green.svg)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Functions-orange.svg)
![License](https://img.shields.io/badge/license-Proprietary-red.svg)

**Enterprise-grade workflow orchestration platform for Mind Hotel Insights**

[Features](#-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Documentation](#-documentation)

</div>

---

## 📋 Overview

**MHI Workflow Orchestrator** is a comprehensive data integration and workflow orchestration platform built on Google Cloud Platform. It provides a robust, scalable solution for managing complex data pipelines across multiple hotel management systems, analytics engines, and business intelligence tools.

### 🎯 Key Capabilities

- **🧠 Intelligent Orchestration**: MSFlowsController acts as the central brain, coordinating all workflows
- **🔄 Dynamic Flow Execution**: N8N-like engine for flexible, code-free workflow definitions
- **📊 Multi-Source Integration**: 7+ pre-built extractors for hotel management systems
- **⚡ Real-time Processing**: Event-driven architecture with Pub/Sub messaging
- **🔍 Advanced Analytics**: Built-in KPI calculation and demand forecasting engines
- **📧 Smart Notifications**: Automated alerts via email and other channels
- **🛡️ Enterprise Security**: Secret management, audit logging, and access control

---

## 🏗️ Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      MHI Workflow Orchestrator                       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
            ┌───────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
            │ MSFlowsController │ Integration │ Analytics  │
            │   (Brain)      │ │  Engines   │ │  Engines   │
            └───────┬──────┘ └─────┬──────┘ └─────┬──────┘
                    │               │               │
        ┌───────────┼───────────────┼───────────────┼───────────┐
        │           │               │               │           │
    ┌───▼───┐  ┌───▼───┐      ┌───▼───┐      ┌───▼───┐  ┌───▼───┐
    │Extract│  │ Model │      │  KPI  │      │Forecast│  │Notify │
    │ ors  │  │ Engine│      │Engine │      │ Engine │  │Engine │
    └───────┘  └───────┘      └───────┘      └───────┘  └───────┘
```

### Core Components

#### 1. **MSFlowsController** 🧠 (The Brain)

The central orchestrator that coordinates all workflows and manages the entire data pipeline lifecycle.

**Responsibilities:**
- Flow lifecycle management (start, execute, monitor, complete)
- Dynamic step execution with dependency resolution
- Callback processing from Cloud Functions
- State persistence and recovery
- Error handling and retry logic

**Key Features:**
- N8N-like workflow engine for visual flow definitions
- Support for conditional logic and loops
- Automatic dependency resolution
- Real-time state tracking
- Integration with all other components

**Technology Stack:**
- Python 3.9+
- Google Cloud Functions (Gen2)
- Cloud Pub/Sub for messaging
- Cloud Storage for state persistence
- Pydantic for configuration validation

[📖 Detailed Documentation](./MSFlowsController/README.md)

---

#### 2. **Integration Engines** 🔌

##### **mhi_integration_engine**

Collection of specialized extractors for various hotel management and business systems.

**Supported Systems:**

| Extractor | System | Description |
|-----------|--------|-------------|
| **MSaxional** | Axional ERP | Financial and accounting data |
| **MSavalon** | Avalon PMS | Hotel property management |
| **MSreviewpro** | ReviewPro | Guest reviews and reputation |
| **MSsharepoint365** | SharePoint | Document management |
| **MSbusinesscentral** | Microsoft BC | Business Central ERP |
| **MSholded** | Holded | Cloud ERP and invoicing |
| **MSmadisa** | Madisa POS | Point of sale data |
| **MSfourvenues** | FourVenues | Event management |
| **MSulysescloud** | Ulyses Cloud | Hotel management |

**Common Features:**
- Incremental data extraction
- Schema validation
- Error handling and retry logic
- Pub/Sub integration
- Cloud Storage persistence

---

#### 3. **Analytics Engines** 📊

##### **MSDataModelingEngine**

Transforms raw data into structured, analytics-ready datasets.

**Capabilities:**
- Data normalization and cleansing
- Schema transformation
- Data quality validation
- Incremental updates
- BigQuery integration

##### **MSKPIEngine**

Calculates business metrics and key performance indicators.

**Features:**
- Pre-built KPI templates
- Custom metric definitions
- Time-series aggregations
- Comparative analysis
- Real-time calculations

##### **MSDemandForecastEngine**

Predictive analytics for demand forecasting.

**Capabilities:**
- ML-based forecasting models
- Seasonal pattern detection
- Anomaly detection
- Multi-horizon predictions
- Confidence intervals

---

#### 4. **Automation & Integration** 🤖

##### **MSQlikAppReload**

Manages Qlik Sense application reloads.

**Features:**
- Scheduled reloads
- On-demand triggers
- Dependency management
- Error notifications

##### **MSQlikAutomationExecution**

Executes Qlik Sense automations.

**Capabilities:**
- Automation triggering
- Parameter passing
- Status monitoring
- Result capture

---

#### 5. **NotificationsEngine** 📧

Multi-channel notification system.

**Channels:**
- Email (Mailgun)
- Slack (planned)
- SMS (planned)
- Webhooks

**Features:**
- Template-based messaging
- Conditional notifications
- Batch processing
- Delivery tracking

---

#### 6. **InternalServices** 🛠️

Shared utilities and common services.

**Components:**
- Authentication & authorization
- Secret management
- Logging utilities
- Common data models
- Helper functions

---

## 🚀 Quick Start

### Prerequisites

```bash
# Required
- Python 3.9+
- Google Cloud SDK
- Active GCP project with billing enabled

# Optional
- Docker (for local development)
- Terraform (for infrastructure)
```

### Installation

```bash
# Clone repository
git clone https://github.com/developcreativo/mhi-workflow-orchestrator.git
cd mhi-workflow-orchestrator

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # For development
```

### Configuration

```bash
# Set environment variables
export _M_PROJECT_ID="your-gcp-project-id"
export _M_ENV_ACRONYM="dev"
export FLOWS_BUCKET="your-flows-bucket"
export RUNS_BUCKET="your-runs-bucket"

# Configure GCP authentication
gcloud auth application-default login
gcloud config set project your-gcp-project-id
```

### Deploy MSFlowsController

```bash
cd MSFlowsController

# Deploy via Cloud Build
gcloud builds submit --config cloudbuild.yaml

# Or deploy manually
gcloud functions deploy MSFlowsController \
  --runtime=python312 \
  --gen2 \
  --region=europe-southwest1 \
  --memory=32GB \
  --cpu=8 \
  --entry-point=FlowWorker \
  --trigger-topic=ms-flows-controller \
  --timeout=540s
```

---

## 📖 Documentation

### Component Documentation

- [MSFlowsController](./MSFlowsController/README.md) - Workflow orchestration brain
- [Integration Extractors](./mhi_integration_engine/README.md) - Data extraction guides
- [Analytics Engines](./docs/analytics-engines.md) - KPI and forecasting
- [Notifications](./NotificationsEngine/README.md) - Alert configuration

### Guides

- [Creating a New Flow](./docs/guides/creating-flows.md)
- [Adding a New Extractor](./docs/guides/new-extractor.md)
- [Monitoring & Troubleshooting](./docs/guides/monitoring.md)
- [Security Best Practices](./docs/guides/security.md)

### API Reference

- [Flow Definition Schema](./docs/api/flow-schema.md)
- [Message Formats](./docs/api/message-formats.md)
- [Callback Protocol](./docs/api/callbacks.md)

---

## 🔄 Workflow Example

### Simple Data Extraction Flow

```json
{
  "flow_id": "daily-avalon-extract",
  "account": "hotel-fergus",
  "name": "Daily Avalon Data Extraction",
  "flow_config": {
    "steps": [
      {
        "id": "extract-reservations",
        "name": "Extract Reservations",
        "type": "ms-extractor-avalon",
        "config": {
          "_m_account": "hotel-fergus",
          "datasets": ["Reservations"],
          "incremental": true
        }
      },
      {
        "id": "model-data",
        "name": "Transform Data",
        "type": "ms-data-modeling",
        "depends_on": ["extract-reservations"],
        "config": {
          "source": "avalon",
          "target_schema": "analytics"
        }
      },
      {
        "id": "calculate-kpis",
        "name": "Calculate KPIs",
        "type": "ms-kpi-engine",
        "depends_on": ["model-data"],
        "config": {
          "metrics": ["occupancy", "adr", "revpar"]
        }
      }
    ]
  },
  "notifications": {
    "success": {
      "type": ["email"],
      "recipients": "data-team@example.com"
    },
    "fail": {
      "type": ["email"],
      "recipients": "alerts@example.com"
    }
  }
}
```

### Triggering the Flow

```bash
# Via gcloud CLI
gcloud pubsub topics publish ms-flows-controller \
  --message-file=flow-definition.json

# Via Python
from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('your-project', 'ms-flows-controller')

flow_data = {...}  # Your flow definition
message = json.dumps(flow_data).encode('utf-8')
publisher.publish(topic_path, message)
```

---

## 🧪 Testing

### Run Unit Tests

```bash
# All tests
pytest tests/ -v

# Specific component
pytest MSFlowsController/tests/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

### Integration Tests

```bash
# Test full flow execution
python scripts/test_flow_execution.py

# Test extractor integration
python scripts/test_extractor.py --extractor=avalon
```

---

## 📊 Monitoring

### Cloud Logging

```bash
# View MSFlowsController logs
gcloud functions logs read MSFlowsController \
  --region=europe-southwest1 \
  --limit=100

# Filter by severity
gcloud functions logs read MSFlowsController \
  --region=europe-southwest1 \
  --filter="severity>=ERROR"
```

### Metrics

Key metrics to monitor:
- Flow execution time
- Success/failure rates
- Extractor performance
- Message queue depth
- Error rates by component

---

## 🛡️ Security

### Secret Management

All secrets are managed via Google Cloud Secret Manager:

```bash
# Store a secret
gcloud secrets create mailgun-api-key \
  --data-file=- <<< "your-api-key"

# Grant access
gcloud secrets add-iam-policy-binding mailgun-api-key \
  --member="serviceAccount:your-sa@project.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Best Practices

- ✅ Never commit secrets to Git
- ✅ Use environment variables for configuration
- ✅ Rotate credentials regularly
- ✅ Use least-privilege IAM roles
- ✅ Enable audit logging
- ✅ Implement secret scanning in CI/CD

---

## 🤝 Contributing

### Development Workflow

1. Create a feature branch
2. Make changes following code standards
3. Run tests and linting
4. Submit pull request
5. Wait for review and approval

### Code Standards

```bash
# Format code
black .

# Lint
ruff check .

# Type checking
mypy .
```

### Commit Messages

Follow conventional commits:
```
feat: add new extractor for system X
fix: resolve timeout issue in MSFlowsController
docs: update README with new examples
refactor: improve error handling in callbacks
```

---

## 📝 License

Proprietary - Mind Hotel Insights © 2025

---

## 📞 Support

- **Documentation**: [docs.mindhotelinsights.com](https://docs.mindhotelinsights.com)
- **Issues**: [GitHub Issues](https://github.com/developcreativo/mhi-workflow-orchestrator/issues)
- **Email**: support@mindhotelinsights.com

---

## 🗺️ Roadmap

### Q1 2025
- [ ] Add Slack notification channel
- [ ] Implement flow versioning
- [ ] Add visual flow designer UI
- [ ] Enhance error recovery mechanisms

### Q2 2025
- [ ] Add more ML-based forecasting models
- [ ] Implement real-time streaming pipelines
- [ ] Add GraphQL API
- [ ] Multi-region deployment

### Q3 2025
- [ ] Add workflow templates marketplace
- [ ] Implement A/B testing for flows
- [ ] Add cost optimization recommendations
- [ ] Enhanced monitoring dashboard

---

<div align="center">

**Built with ❤️ by the Kelly Development Team**

[⬆ Back to Top](#mhi-workflow-orchestrator)

</div>
