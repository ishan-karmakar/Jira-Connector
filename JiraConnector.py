### Imports

from jira import JIRA
from jira.resources import Attachment
import os
from dotenv import load_dotenv
import time
import requests
import json


load_dotenv()


### Environemnt Variables
JIRA_USERNAME = os.getenv("JIRA_USERNAME")
JIRA_PASSWORD = os.getenv("JIRA_PASSWORD")
JIRA_SERVER = os.getenv("JIRA_SERVER")
AUTOCAM_APIKEY = os.getenv("AUTOCAM_APIKEY")


### JIRA Setup
jira = JIRA(
    server=JIRA_SERVER,
    basic_auth=(JIRA_USERNAME, JIRA_PASSWORD),
)

### AutoCAM Setup
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {AUTOCAM_APIKEY}"})


### Helper Functions
def getJiraIssues():
    issues = jira.search_issues(
        'project = Hardware AND assignee = Empty AND status = "Ready to Fabricate"  and Machinery = "CNC Router"'
    )
    return issues


def handlePostgresPartCategories(Material, Thickness):
    response = session.get("http://localhost:3000/api/pc")
    part_categories = response.json()
    for pc in part_categories:
        if pc["material"] == Material and pc["thickness"] == Thickness:
            return pc["id"]
    response = session.post(
        "http://localhost:3000/api/pc",
        json={"material": Material, "thickness": Thickness},
    )
    category_id = response.json().get("id")
    return category_id


def handlePostgresParts(Name, Epic, Ticket, Quantity, category_id, attachment):
    parts = session.get(f"http://localhost:3000/api/pc/{category_id}/parts").json()
    for part in parts:
        if part["ticket"] == Ticket:
            return
    response = session.post(
        f"http://localhost:3000/api/pc/{category_id}/parts",
        files={
            "data": (
                None,
                json.dumps(
                    {
                        "name": Name,
                        "epic": Epic,
                        "ticket": Ticket,
                        "quantity": Quantity,
                    }
                ),
                "application/json",
            ),
            "file": (attachment.filename, attachment.get(), "application/octet-stream"),
        },
    )
    if response.ok:
        try:
            print(response.json())
        except requests.exceptions.JSONDecodeError:
            print(f"Part created successfully, but response was not JSON: {response.text}")
    else:
        print(f"Failed to create part. Status: {response.status_code}, Response: {response.text}")


def cleanUpOldParts(issue_keys: set[str]):
    if not issue_keys:
        print("No JIRA issues returned; skipping cleanup to avoid deleting everything.")
        return
    deleted_parts = []
    for pc in session.get("http://localhost:3000/api/pc").json():
        parts = session.get(f"http://localhost:3000/api/pc/{pc['id']}/parts").json()
        for part in parts:
            if part["ticket"] not in issue_keys:
                deleted_parts.append(part)
                session.delete(f"http://localhost:3000/api/parts/{part['id']}")
        if len(parts) == 0:
            session.delete(f"http://localhost:3000/api/pc/{pc['id']}")


### Main Function
def processJiraIssues():
    issues = getJiraIssues()
    issue_keys = {issue.key for issue in issues}

    print("issues found:", len(issues))

    processed = 0
    for issue in issues:
        Epic = jira.issue(issue.get_field("customfield_10110")).get_field("summary")
        Name = issue.get_field("summary")
        Quantity = int(issue.get_field("customfield_10206"))
        Ticket = issue.key
        Material = str(issue.get_field("customfield_10202"))
        Thickness = float(str(issue.get_field("customfield_10207")))
        attachments = issue.get_field("attachment")
        attachments = [att for att in attachments if att.filename.endswith(".step")]
        if (
            not Material
            or not Thickness
            or not Name
            or not Epic
            or not Quantity
            or Material == ""
            or Thickness == 0
            or Name == ""
            or Epic == ""
            or Quantity == 0
            or len(attachments) == 0
        ):
            continue
        category_id = handlePostgresPartCategories(Material, Thickness)
        handlePostgresParts(Name, Epic, Ticket, Quantity, category_id, attachments[0])
        processed += 1

    print(f"Finished processing issues. {processed} processed.")
    cleanUpOldParts(issue_keys)


if __name__ == "__main__":
    while True:
        processJiraIssues()
        time.sleep(60)
