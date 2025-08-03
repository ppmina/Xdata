### **Revised Project Enhancement Plan (v2)**

**Objective:** To refactor the data download and export functionalities for better flexibility, implement a true incremental download mechanism, and introduce a seamless database query feature.

---

#### **Part 1: Implement True Incremental Data Download**

*   **Goal:** To modify the download process to support an `incremental` mode. When enabled, the system will intelligently download only the data that is missing. This covers two key scenarios:
    1.  **Adding New Fields:** If a new data field (e.g., a new metric) is added to the configuration, the system will download *only that new field* for the existing date range, leaving previously downloaded data untouched.
    2.  **Filling Data Gaps:** The system will automatically identify and download data for any dates that are missing within the database for a given period.
*   **Implementation Strategy:**
    *   I will introduce a new boolean parameter, `incremental: bool = True`, to the main download function in `market_service.py`.
    *   The core logic will be in `base_downloader.py`. Before starting a download, it will:
        1.  Query the database to get a summary of which dates and fields already exist for a given symbol.
        2.  Compare this with the list of required dates and fields from the user's request and configuration.
        3.  Generate a precise "download plan" of only the missing data points (specific fields for specific dates).
    *   The individual downloaders (e.g., `kline_downloader.py`) will then execute this plan.
*   **Affected Files:**
    *   `src/cryptoservice/services/market_service.py`: Add the new `incremental` parameter to the public download methods.
    *   `src/cryptoservice/services/downloaders/base_downloader.py`: Implement the core logic for detecting and planning the download of missing data.
    *   `src/cryptoservice/storage/async_storage_db.py`: Add a new method to efficiently report which data points (fields and dates) are already stored.

---

#### **Part 2: Enhance Data Export Functionality**

*   **Goal:** Add `start_date` and `end_date` parameters to the data export function to allow for partial data exports.
*   **Affected Files:**
    *   `src/cryptoservice/storage/async_export.py`: Modify the core export functions to accept `start_date` and `end_date` arguments.
    *   `src/cryptoservice/storage/storage_db.py`: Update the database query within the export logic to filter the dataset by the given date range.

---

#### **Part 3: Update Demo Scripts**

*   **Goal:** Provide clear examples of how to use the new and updated features.
*   **Affected Files:**
    *   `demo/download_data.py`: Update to demonstrate the new `incremental` download mode.
    *   `demo/export_data.py`: Update to show how to use `start_date` and `end_date` for partial exports.
    *   `demo/read_db.py`: Enhance to be a comprehensive example of the new "seamless" query feature.
