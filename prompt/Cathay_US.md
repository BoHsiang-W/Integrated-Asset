Extract only the Transaction Details of Offshore Stocks from the attached PDF and present them as markdown tables.

Instructions:
- Only include tabular data; ignore unrelated content.
- Exclude all other data.
- Only include rows where the Product Code is 6 characters or fewer.
- Present the extracted data as markdown tables, following the specified column formats for each Trade Type.

- For **Trade Type = "買進"** rows, include: Product Code, Trade Type, Number of Shares, Prices, Trading Amount, Handling Fee, Account Receivable, Settlement Date.
For **買進**:
| Settlement Date | Trade Type | Product Code | Number of Shares | Prices | Trading Amount | Handling Fee | Account Receivable |

- For **Trade Type = "除息"** rows, include: Product Code, Trade Type, Account Payable, Settlement Date.
For **除息**:
| Settlement Date | Trade Type | Product Code | Account Payable |