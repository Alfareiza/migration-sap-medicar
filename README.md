<h2 align="center">Data Migration Plan</h2>
<h4 align="center">Medicar to SAP Business</h4>
<h2 align="center">
<img alt="GitHub followers" src="https://img.shields.io/github/followers/Alfareiza?label=Follow%20me%20%3A%29&style=social">
</h2>

----

## Overview

This project involves a data migration plan to transfer data from the "Medicar" software to "SAP Business one". As both systems have different data models and formats, a carefully crafted approach is required throughout the migration.


### Strategy: Trickle Data Migration

I'm adopting a Trickle Data Migration strategy, where the old and new systems run concurrently, allowing data to be moved in small increments. This avoids downtime and operational interruptions, enabling a seamless transition. The migration is performed iteratively to ensure a sequential and secure transfer of all information.

### Method: Hand Coding with Python

To achieve this, we are utilizing Hand Coding, meaning the migration process is entirely managed by Python scripts. As developers, we read, process, and execute the migration, ensuring control and customization throughout.

### Technologies and Concepts:

- **Medicar**: The source software from which data is being migrated.
- **SAP**: The target software receiving the migrated data.
- **SAP API**: API used to register the information.
- **Google Drive API**: API used to scrap the Medicar files in order to process them to send the info to SAP API.
- **Django**: The migration plan is managed through a Django command, granting access to web resources.
- **Heroku**: The migration is executed periodically from a Heroku server, ensuring consistent and reliable data transfers.

By following this data migration plan, I aim to seamlessly transition from medicar to SAP, efficiently adapting to new application interactions and infrastructure, while maintaining data integrity and ensuring a smooth experience for all users.

Data migration plan to transfer data from the "Medicar" software to "SAP Business one". Using Google Drive API and SAP API the data is transferred considering that both systems have different data models and formats.