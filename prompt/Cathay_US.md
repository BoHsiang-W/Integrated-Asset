Extract only the Transaction Details of Offshore Stocks from the attached PDF and return them as CSV.

Instructions:
- Only include transaction data; ignore unrelated content.
- Exclude all other data.
- Only include rows where the Product Code is 6 characters or fewer.
- Output ONLY raw CSV text. No markdown, no code fences, no explanations.
- Use the EXACT column headers specified below as the first row.
- Use comma as delimiter. Wrap fields containing commas in double quotes.

CSV columns (use this exact header row):
交易日期,買/賣/股利,代號,股票,交易類別,買入股數,買入價格,賣出股數,賣出價格,現價,手續費,折讓後手續費,交易稅,成交價金,交易成本,支出,收入,決策原因,手續費折數

Rules:
- 交易類別 = "ETF" if the product code matches known ETF patterns (e.g., VOO, QQQ, VGT). Otherwise, use "一般".
- 代號 = the product/ticker code (e.g. VGT, AAPL). 股票 = same as 代號 (e.g. VGT, AAPL).
- Map Trade Types: "買進" → "買", "賣出" → "賣", "除息" → "股利".
- For "買": fill 買入股數 and 買入價格, leave 賣出股數, 賣出價格, and 收入 empty.
- For "賣": fill 賣出股數 and 賣出價格, leave 買入股數, 買入價格, and 收入 empty.
- For "股利": find the numeric value in the "Actual Account Receivable / Payable" or "實付金額" column on the same row as the dividend transaction. Fill 收入 with this amount (use absolute value, no negative sign). Leave all quantity/price columns empty.
- 折讓後手續費: the actual charged fee after discount if available, otherwise leave empty.
- 交易稅: leave empty (not applicable for offshore stocks).
- Leave 現價, 手續費, 成交價金, 交易成本, 支出, 決策原因, 手續費折數 empty — these are not available in the PDF.