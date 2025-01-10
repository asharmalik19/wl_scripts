VENDOR FINDER BOT - Quick Reference

INPUT: Excel file with lead_ids and websites
OUTPUT: Excel file with vendor matches

Key Functions:
- process_website(): Main orchestrator for each website
- check_vendors_on_page(): Scans single page for vendors
- get_all_internal_links(): Extracts internal links (max 30)
- make_request(): Handles HTTP requests with retries

Flow:
1. Reads websites from Excel
2. For each website:
   - Checks homepage for vendors
   - Extracts internal links
   - Checks each internal link for vendors
3. Uses ThreadPoolExecutor for parallel processing
4. Saves results to Excel

Limitations:
- 202 responses not handled
- Some working websites missed
