# Jira Connector for AutoCAM

An example integration that syncs CNC Router fabrication jobs from Jira to AutoCAM.

## Overview

This connector monitors a Jira Hardware project for issues marked "Ready to Fabricate" and automatically syncs them to AutoCAM. It extracts part information including material, thickness, quantity, and STEP file attachments, organizing them into part categories for CNC routing.

## Features

- **Automatic Sync**: Polls Jira every 60 seconds for new fabrication jobs
- **Part Categories**: Automatically organizes parts by material and thickness
- **STEP File Support**: Extracts and uploads `.step` file attachments
- **Cleanup**: Removes parts from AutoCAM when their corresponding Jira tickets are no longer in the queue

## Requirements

- Python 3.x
- A running AutoCAM instance at `localhost:3000`
- Jira account with access to the Hardware project

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
JIRA_USERNAME=your_jira_email
JIRA_PASSWORD=your_jira_api_token
JIRA_SERVER=https://your-domain.atlassian.net
AUTOCAM_APIKEY=your_autocam_api_key
```

## Usage

Run the connector:

```bash
python JiraConnector.py
```

The connector will continuously poll Jira and sync matching issues to AutoCAM.

## How It Works

1. Queries Jira for issues matching:
   - Project: `Hardware`
   - Assignee: `Empty`
   - Status: `Ready to Fabricate`
   - Machinery: `CNC Router`

2. For each issue, extracts:
   - Part name (summary)
   - Epic name
   - Ticket key
   - Quantity
   - Material
   - Thickness
   - STEP file attachment

3. Creates or updates part categories in AutoCAM based on material/thickness combinations

4. Uploads parts with their STEP files to the appropriate category

5. Cleans up any parts in AutoCAM that no longer have corresponding Jira tickets

## Jira Custom Fields

This connector expects the following custom fields on Jira issues:

| Field | Custom Field ID | Description |
|-------|-----------------|-------------|
| Epic Link | `customfield_10110` | Link to parent epic |
| Quantity | `customfield_10206` | Number of parts to fabricate |
| Material | `customfield_10202` | Material type (e.g., Aluminum, Steel) |
| Thickness | `customfield_10207` | Material thickness |

## License

MIT

