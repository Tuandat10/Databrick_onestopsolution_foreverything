# Admin Guide: Azure Databricks Lakehouse Access & Governance

This document provides an in-depth description of how user access, group-based permissions, and data governance are structured and enforced in the Azure Databricks Lakehouse implementation. Rather than only showing how to do tasks, it explains why each governance choice exists.

---

## 🧑‍💼 User and Group Management Overview

Access to the Databricks workspace and data catalog is managed through integration with **Microsoft Entra ID (Azure Active Directory)**. Users are granted access via groups, each with specific responsibilities and permission levels.

### Key Groups

- **`admins`**
  - Full administrative rights across Databricks and Unity Catalog.
  - Can create schemas, manage masking policies, grant/revoke access.
  - Includes platform engineers or data governance officers.

- **`data_analysis`**
  - Analysts and business users.
  - Granted read-only access to gold-layer tables.
  - Subject to masking for sensitive fields.

Group assignment is evaluated dynamically by Unity Catalog during data access.

---

## 🔐 Unity Catalog Permission Structure

Unity Catalog enforces access control across all data layers (catalogs, schemas, and tables).

### Typical Permissions per Group

```sql
-- Catalog-level access
GRANT USE CATALOG ON CATALOG metacatalog TO `data_analysis`;

-- Schema-level access
GRANT USE SCHEMA ON SCHEMA metacatalog.goldschema TO `data_analysis`;

-- Table-level access
GRANT SELECT ON ALL TABLES IN SCHEMA metacatalog.goldschema TO `data_analysis`;

-- Optional: Allow UI table browsing
GRANT BROWSE ON SCHEMA metacatalog.goldschema TO `data_analysis`;
```
## 🎭 Data Masking Strategy

To protect personal data, Unity Catalog implements **column-level masking** for fields such as:

- `Gender`
- `Age`
- `Country`

### Masking Behavior

- Users in the `admins` group will see the full, unmasked data.
- Users in the `data_analysis` group will see masked values (e.g., `NULL`, `"Restricted"`).
- These rules are defined using `SET MASKING POLICY` in the notebook `SET_MASKING.ipynb`.

This approach ensures compliance with internal data governance standards and regulatory requirements such as GDPR or company-specific policies.
## 🧠 Governance Intent

The architecture follows these principles:

- **Least Privilege**: Only essential access is granted.
- **Separation of Duties**: Admins administer; analysts consume.
- **Data Protection**: PII is masked unless needed.
- **Centralized Control**: Unity Catalog manages all workspace data access.

---

## 🐞 Common Access Issues Explained

| Scenario                         | Cause                    | Explanation                                                                 |
|----------------------------------|---------------------------|------------------------------------------------------------------------------|
| User can’t view SQL Editor       | Missing entitlement       | Enable “Databricks SQL Access” in user settings.                            |
| Queries return `INSUFFICIENT_PRIVILEGES` | Permissions not granted | Ensure `USE SCHEMA`, `SELECT`, and `BROWSE` are granted in Unity Catalog.   |
| Fields like `Gender` show NULL   | Masking in effect         | The user isn’t in a group with unmasked access.                             |
| User can’t enter workspace       | Not added to workspace    | Add the user to the workspace via Admin Console.                            |
## 📋 Governance Checklist

- [x] User exists in Azure Active Directory
- [x] User added to Databricks workspace
- [x] `Databricks SQL Access` entitlement assigned
- [x] User assigned to correct group (e.g., `data_analysis`)
- [x] Catalog, schema, and table permissions granted
- [x] Masking behavior verified with test queries

