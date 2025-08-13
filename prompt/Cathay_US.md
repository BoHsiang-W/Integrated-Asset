Extract only the Transaction Details of Offshore Stocks from the attached PDF and present them as markdown tables.

Instructions:
- Only include tabular data; ignore unrelated content.
- For **Trade Type = "買進"** rows, include: Product Code, Trade Type, Number of Shares, Prices, Trading Amount, Handling Fee, Account Receivable, Settlement Date.
- For **Trade Type = "除息"** rows, include: Product Code, Trade Type, Account Payable, Settlement Date.
- Exclude all other data.
- Format output as markdown tables:

For **買進**:
| Settlement Date | Trade Type | Product Code | Number of Shares | Prices | Trading Amount | Handling Fee | Account Receivable |

For **除息**:
| Settlement Date | Trade Type | Product Code | Account Payable |