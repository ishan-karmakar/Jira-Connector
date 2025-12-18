### Imports

from jira import JIRA, Project
from jira.resources import Attachment
import os
from dotenv import load_dotenv
import boto3
import sqlalchemy
from sqlalchemy import create_engine, Table, select, MetaData
import time

load_dotenv()


### Environemnt Variables
JIRA_USERNAME = os.getenv("JIRA_USERNAME")
JIRA_PASSWORD = os.getenv("JIRA_PASSWORD")
JIRA_SERVER = os.getenv("JIRA_SERVER")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

### Database Setup
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}/{POSTGRES_DB}"
)
metadata = MetaData()
part_categories = Table("part_categories", metadata, autoload_with=engine)
plates = Table("parts", metadata, autoload_with=engine)

### JIRA Setup
jira = JIRA(
    server=JIRA_SERVER,
    basic_auth=(JIRA_USERNAME, JIRA_PASSWORD),
)


### Helper Functions
def getJiraIssues():
    issues = jira.search_issues(
        'project = Hardware AND assignee = Empty AND status = "Ready to Fabricate"  and Machinery = "CNC Router"'
    )
    return issues


def handleS3withIssue(issues, Name):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("autocam-attachments")
    attachments = issues.get_field("attachment")
    if not attachments:
        return
    for attachment in attachments:
        if attachment.filename.endswith(".step"):
            Key = f"Valor-{Name}-{attachment.filename}"
            if not any(obj.key == Key for obj in bucket.objects.all()):
                bucket.put_object(Key=Key, Body=attachment.get())
            break


def handlePostgresPartCategories(Material, Thickness):
    stmt = select(part_categories).where(
        part_categories.c.material == Material, part_categories.c.thickness == Thickness
    )
    with engine.begin() as conn:
        result = conn.execute(stmt).fetchone()

        if result is None:
            ins = (
                part_categories.insert()
                .values(material=Material, thickness=Thickness)
                .returning(part_categories.c.id)
            )
            category_id = conn.execute(ins).scalar_one()
        else:
            category_id = result._mapping["id"]
    return category_id


def handlePostgresParts(Name, Epic, Ticket, Quantity, category_id):
    stmt = select(plates).where(plates.c.name == Name)
    with engine.begin() as conn:
        result = conn.execute(stmt).fetchone()
        if result is None:
            conn.execute(
                plates.insert().values(
                    name=Name,
                    epic=Epic,
                    ticket=Ticket,
                    quantity=Quantity,
                    category_id=category_id,
                )
            )


def cleanUpOldParts(issue_keys: set[str]):
    if not issue_keys:
        print("No JIRA issues returned; skipping cleanup to avoid deleting everything.")
        return

    stmt = select(plates).where(~plates.c.ticket.in_(list(issue_keys)))
    with engine.begin() as conn:
        results = conn.execute(stmt).fetchall()
        for result in results:
            delete_stmt = plates.delete().where(plates.c.id == result._mapping["id"])
            conn.execute(delete_stmt)

    stmt = select(part_categories).where(
        ~part_categories.c.id.in_(select(plates.c.category_id).distinct())
    )
    with engine.begin() as conn:
        results = conn.execute(stmt).fetchall()
        for result in results:
            delete_stmt = part_categories.delete().where(
                part_categories.c.id == result._mapping["id"]
            )
            conn.execute(delete_stmt)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket("autocam-attachments")
    for obj in bucket.objects.all():
        key = obj.key
        if not key.startswith("Valor-"):
            continue

        rest = key[len("Valor-") :]
        if "-" not in rest:
            continue
        part_name, _filename = rest.rsplit("-", 1)

        stmt = select(plates).where(plates.c.name == part_name)
        with engine.begin() as conn:
            result = conn.execute(stmt).fetchone()
            if result is None:
                obj.delete()


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

        if not Material or not Thickness or not Name or not Epic or not Quantity:
            continue

        handleS3withIssue(issue, Name)
        category_id = handlePostgresPartCategories(Material, Thickness)
        handlePostgresParts(Name, Epic, Ticket, Quantity, category_id)
        processed += 1

    print(f"Finished processing issues. {processed} processed.")
    cleanUpOldParts(issue_keys)


if __name__ == "__main__":
    while True:
        processJiraIssues()
        time.sleep(60)
