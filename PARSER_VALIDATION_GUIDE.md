# Portfolio Parser - Critical Validation Rules

## Document Purpose
This document consolidates all parser bugs discovered and fixed during extensive testing. Use this as a validation checklist when making parser changes or testing new statements.

---

## Table of Contents
1. [TDA Parser Rules](#tda-parser-rules)
2. [Schwab Parser Rules](#schwab-parser-rules)
3. [Number Extraction Rules](#number-extraction-rules)
4. [Symbol Extraction Rules](#symbol-extraction-rules)
5. [Transaction Type Classification](#transaction-type-classification)
6. [Amount Standardization](#amount-standardization)
7. [Common Edge Cases](#common-edge-cases)

---

## TDA Parser Rules

### 1. Line Wrapping & Continuation
**Issue**: PDF extraction sometimes splits transactions across multiple lines.

**Pattern to Handle**:
```
Line 1: 07/05/23 Buy - Securities Purchased VANGUARD VOO 407.4041 (240.12) 23,037.29
Line 2: S&P 500 ETF SHS 0.589
```

**Solution**: Merge continuation lines (lines 1271-1295)
- Detect: No date at start AND (ends with decimal OR matches company name pattern)
- Patterns: `\d+\.\d+\s*$` or `^[A-Z&\s]+\d`

**Test Cases**:
- ✅ VOO with "S&P 500 ETF SHS 0.589" on next line
- ✅ VB with "SMALL CP ETF" on next line
- ✅ QQQ with "UNIT SER 1 ETF" on next line

### 2. Quantity/Price Order
**Issue**: Numbers can appear in different orders depending on line wrapping.

**Correct Formats**:
```
Format A: Symbol QUANTITY PRICE (AMOUNT) BALANCE
Format B: Symbol PRICE (AMOUNT) BALANCE [newline] Description QUANTITY
```

**Solution**: Use mathematical validation (lines 1678-1717)
- Calculate: quantity × price ≈ amount (within 5% tolerance)
- Use financial heuristics based on transaction amount

**Financial Heuristics**:
- **Small amounts (< $500)**: Dividend reinvestments → prefer small quantity (< 10 shares)
- **Large amounts (≥ $500)**: Regular purchases → allow large quantities, prefer reasonable prices (< $1000)

**Test Cases**:
- ✅ VOO: 0.589 shares @ $407.40 = $240.12 (small amount, small qty)
- ✅ VTI: 1.627 shares @ $153.05 = $248.96 (small amount, small qty)
- ✅ CCL: 1,282 shares @ $16.36 = $20,973 (large amount, large qty OK)
- ✅ TSM: 1.219 shares @ $112.75 = $137.40 (small amount, small qty)
- ✅ INTC: 2.716 shares @ $36.25 = $98.46 (small amount, small qty)
- ✅ **NVDA: 100 shares @ $199.59 = $19,959 (whole number preferred over 199.59 shares @ $100)**

### 3. Fee Transactions
**Issue**: "FEE ON 10 SHARES" being parsed as ticker symbol "ON".

**Pattern**:
```
Journal - Expense FEE ON 10 SHARES FOR SELL
```

**Solution**:
- Detect "Journal - Expense" → set type = 'FEE' (line 1394)
- Exclude FEE/OTHER from symbol extraction (line 1467)
- Add skip words: 'FEE', 'ON', 'FOR', 'SHARES', 'SHORT', 'CENTER', 'MARKET' (line 1489)

**Test Cases**:
- ✅ 2017-11-08: FEE with no symbol, amount = -$19.99
- ❌ Should NOT extract "ON" as symbol

---

## Schwab Parser Rules

### 1. Dividend Reinvestments
**Issue**: "Purchase ReinvestedShares" being parsed as BUY instead of DIVIDEND.

**Pattern**:
```
10/01 Purchase Reinvested Shares VOO VANGUARD S&P 500 ETF 0.4415 520.8799 (229.95)
```

**Solution**: Skip these in stock transaction parsing (line 925-929)
- Detect: 'reinvested' AND 'shares' in lowercase line
- These should be categorized as DIVIDEND, not BUY

**Test Cases**:
- ✅ Reinvested dividends should not appear as BUY transactions
- ✅ Regular "Purchase" without "Reinvested" → BUY

### 2. Stock Splits
**Issue**: Stock splits must be distinguished from dividend reinvestments.

**Key Difference**:
- **Stock splits**: price = 0.0 (no cost)
- **Dividend reinvestments**: price > 0 (purchased with dividend)

**Patterns**:
```
Other Activity Forward Split SMCI SUPER MICRO COMPUTER INC 1,070.0000
Other Activity ForwardSplit SMCI (no spaces)
Other Activity Stock Split NVDA
Other Activity StockSplit NVDA (no spaces)
```

**Solution**: Check for split keywords in various formats (lines 940-997)
- Patterns: 'forward split', 'forwardsplit', 'stock split', 'stocksplit'
- Always keep as SPLIT type regardless of quantity

**Test Cases**:
- ✅ SMCI split: 1,070 shares, price = 0
- ✅ NVDA split: various fractional amounts, price = 0

### 3. Disclaimer Text Filtering
**Issue**: Legal text being parsed as transactions.

**Problematic Patterns**:
```
"Real Estate Investment Trust (REIT) securities" → Symbol: REIT
"FDIC insured banks" → Symbol: FDIC  
"call us at 800-515-2157" → Amount: $2157
```

**Solution**: Added disclaimer filters in detailed parser (lines 686-704)
```python
disclaimer_indicators = [
    'fdic insured',
    'real estate investment trust',
    'this information is not a solicitation',
    'obligations of one or more',
    'schwab does not provide tax advice',
    'securities purchased on margin',
    'not obligations of schwab',
    'deposit insurance from the fdic',
    'further information on these transactions',
    'you can lose more funds',
    'advisor, if applicable'
]
```

**Test Cases**:
- ✅ Should NOT create transactions from phone numbers
- ✅ Should NOT extract symbols from legal disclaimers
- ✅ REIT/FDIC disclaimer text properly filtered

---

## Number Extraction Rules

### 1. Comma-Separated Thousands
**Issue**: "1,282" being parsed as two numbers: "1" and "282".

**Pattern**: `(?<!\.)([\d,]+)(?!\.)`

**Solution**: Updated regex to include commas in integer matching (line 1591)

**Before**: `(\d+)` - matches only digits
**After**: `([\d,]+)` - matches digits WITH commas

**Test Cases**:
- ✅ 1,282 → parsed as 1282 (single number)
- ✅ 1,792 → parsed as 1792
- ✅ 20,973.52 → parsed as 20973.52

### 2. Combined Number Pattern
**Comprehensive regex** (line 1591):
```python
combined_pattern = r'([\d,]+\.?\d*)-(?!\d)|(\([\d,]+\.?\d*\))|([\d,]+\.\d+)|(?<!\.)([\d,]+)(?!\.)'
```

**Matches** (in priority order):
1. Trailing dash: `59-`, `0.206-` → negative quantity
2. Parentheses: `(98.46)`, `(20,973.52)` → negative amount
3. Decimals: `99.50`, `5,870.36` → prices/amounts
4. Integers with commas: `131`, `1,282`, `100` → quantities

**Test Cases**:
- ✅ Extract all number formats correctly
- ✅ Preserve negative indicators
- ✅ Handle comma separators

---

## Symbol Extraction Rules

### 1. Symbol Position Priority
**Issue**: Symbols after numbers (from wrapped lines) being selected.

**Solution**: Prefer symbols BEFORE first number (lines 1503-1528)

**Logic**:
```python
first_num_pos = position of first digit in line
symbols_before_numbers = [symbols before first_num_pos]
symbols_after_numbers = [symbols after first_num_pos]

# Prefer symbols before numbers (ticker appears between company name and numbers)
if symbols_before_numbers:
    select from symbols_before_numbers
else:
    select from symbols_after_numbers
```

**Test Cases**:
- ✅ "VANGUARD VB 34- ... SMALL CP ETF" → VB (not CP)
- ✅ "INVESCO QQQ ... UNIT SER 1 ETF" → QQQ (not SER)
- ✅ "VANGUARD VOO 0.589 407.4041 ... S&P 500 ETF" → VOO (not ETF)

### 2. Skip Words List
**Comprehensive list** (lines 1477-1491):
```python
skip_words = {
    'CORP', 'INC', 'COM', 'ETF', 'SHS', 'CASH', 'FUND', 'TRUST', 'CLASS',
    'CREDIT', 'FOREIGN', 'QUALIFIED', 'DIVIDENDS', 'INTEREST',
    'WIRE', 'FDIC', 'OUT', 'IN', 'CO', 'TRANSFER', 'ACH', 'PURCHASE',
    'REDEMPTION', 'ACCOUNT', 'HARRY', 'SELL', 'BUY', 'JOURNALED',
    'FUNDS', 'DISBURSED', 'DEPOSITED', 'INSURED', 'DEPOSIT', 'TO',
    'REAL', 'ESTATE', 'TOTAL', 'STK', 'MKT', 'ADR', 'MANUFACTU',
    'FROM', 'SOLD', 'PURCHASED', 'US', 'GLB', 'EX', 'SECTOR',
    'SPDR', 'SELECT', 'ENERGY', 'UNITS', 'INDEX', 'SMALL', 'CAP',
    'REIT', 'VANGUARD', 'ADJ', 'NRA', 'TAIWAN', 'SEMICONDUCTOR',
    'GLOBAL', 'CARNIVAL', 'ID', 'INTL', 'SCHWAB',
    'FEE', 'ON', 'FOR', 'SHARES', 'SHORT', 'CENTER', 'MARKET'
}
```

**Test Cases**:
- ✅ "SMALL CAP" → don't extract "CAP" or "SMALL"
- ✅ "FEE ON 10 SHARES" → don't extract "ON"
- ✅ "S&P 500 ETF" → don't extract "ETF"

### 3. No Symbol for Non-Stock Transactions
**Transaction types without symbols** (line 1467):
```python
if transaction_type in ['WITHDRAWAL', 'DEPOSIT', 'JOURNAL', 'TRANSFER', 
                        'INTEREST', 'TAX', 'FEE', 'OTHER']:
    symbol = ''  # Don't extract symbol
```

**Test Cases**:
- ✅ Fee transactions: no symbol
- ✅ Journal transfers: no symbol
- ✅ Interest/tax: no symbol

---

## Transaction Type Classification

### TDA Transaction Types (lines 1376-1399)

| Statement Text | Transaction Type | Notes |
|---------------|-----------------|-------|
| Buy - Securities Purchased | BUY | Stock purchase |
| Sell - Securities Sold | SELL | Stock sale |
| Div/Int - Income + QUALIFIED DIVIDENDS | DIVIDEND | Qualified dividend |
| Div/Int - Income + INTEREST CREDIT | INTEREST | Interest income |
| Div/Int - Income (other) | DIVIDEND | Regular dividend |
| Journal - Other + FOREIGN WITHHOLDING | TAX | Foreign tax |
| Journal - Other (other) | JOURNAL | Other journal entry |
| Journal - Expense | **FEE** | Short selling fee, etc. |
| Journal - Funds Disbursed | WITHDRAWAL | ACH out, transfer |
| Delivered | TRANSFER | Account transfer |
| Deposit | DEPOSIT | ACH in |

### Schwab Transaction Types (lines 922-997)

| Statement Text | Transaction Type | Notes |
|---------------|-----------------|-------|
| Sale / Sold / Sell | SELL | Stock sale |
| Purchase / Buy / Bought | BUY | Stock purchase |
| **Purchase ReinvestedShares** | **SKIP** | Treat as DIVIDEND, not BUY |
| Withdrawal | WITHDRAWAL | Cash out |
| Deposit | DEPOSIT | Cash in |
| Qualified Dividend | DIVIDEND | Qualified dividend |
| Dividend | DIVIDEND | Regular dividend |
| Long Term Gain | DIVIDEND | Capital gain distribution |
| MoneyLink / Wire | TRANSFER | Money transfer |
| AccountTransfer | TRANSFER | Account transfer |
| **ForwardSplit / StockSplit** | **SPLIT** | Stock split (price = 0) |

---

## Amount Standardization

### Cash Flow Perspective (lines 271-295)

**Negative amounts** (cash going out):
- BUY
- WITHDRAWAL
- TAX
- **FEE**

**Positive amounts** (cash coming in):
- SELL
- DEPOSIT
- DIVIDEND
- INTEREST
- JOURNAL
- OTHER

**Validation**:
```python
# Ensure amounts have correct sign based on transaction type
if transaction_type in negative_types:
    amount = -abs(amount)
elif transaction_type in positive_types:
    amount = abs(amount)
```

---

## Common Edge Cases

### 1. Dividend Reinvestment vs Regular Purchase
**Key Distinction**:
- Small quantity (0.01-10 shares)
- Small amount (< $500)
- Triggered by dividend payment on same date

**Validation**:
- Check if amount < $500 → prefer quantity < 10
- Look for matching dividend transaction on same date

### 2. Stock Split vs Dividend Reinvestment
**Key Distinction**:
- **Stock split**: price = 0.0, description contains "split"
- **Dividend reinvest**: price > 0, triggered by dividend

**Validation**:
- Check price field
- Check description for split keywords
- Verify no dividend payment associated

### 3. Fractional Share vs Whole Share Transactions
**Both are valid**:
- Fractional: Dividend reinvestments, dollar-based purchases
- Whole: Traditional purchases

**Validation**:
- Don't reject based solely on quantity being fractional
- Verify quantity × price ≈ amount

### 4. Large Quantity Purchases (Cheap Stocks)
**Example**: CCL during COVID crash
- 1,282 shares @ $16.36 = $20,973

**Validation**:
- Don't assume small quantity always correct
- For amounts > $1000, allow large quantities if price < $100

### 5. Security Transfers Between Brokers
**Pattern**: Same-day "Delivered" (TDA) and "AccountTransfer" (Schwab)

**Validation**:
- Both should be type = TRANSFER
- Quantities should match
- One has negative qty (from TDA), one has positive qty (to Schwab)

---

## Parser Architecture Changes

### 1. PDF Extraction (lines 183-231)
**Upgraded**: pdftotext → **pdfplumber**

**Rationale**:
- Better table structure preservation
- More accurate text positioning
- Handles complex layouts better

**Fallback**: If pdfplumber fails, use pdftotext

### 2. Parallel Processing (lines 2173-2257)
**Added**: ProcessPoolExecutor for concurrent statement processing

**Benefits**:
- 3-4x faster processing
- Uses multiple CPU cores
- Progress tracking with tqdm

**Configuration**:
```python
max_workers = max(1, multiprocessing.cpu_count() - 1)
```

---

## Testing Checklist

When validating parser changes, test these scenarios:

### TDA Statements
- [ ] Dividend reinvestments with wrapped lines (VOO, VTI, VB)
- [ ] Fee transactions (shouldn't extract "ON" as symbol)
- [ ] Large quantity purchases with commas (CCL 1,282 shares)
- [ ] Small fractional purchases (NVDA 0.017 shares)
- [ ] Foreign stocks with tax (TSM dividends)
- [ ] Multiple transactions on same date (duplicates check)

### Schwab Statements
- [ ] Dividend reinvestments (should be DIVIDEND, not BUY)
- [ ] Stock splits (SMCI, NVDA - price should be 0)
- [ ] Account transfers (should be TRANSFER)
- [ ] Money market interest (small amounts)
- [ ] Legal disclaimer text (should NOT create transactions)

### Number Extraction
- [ ] Comma-separated thousands (1,282 not 1 and 282)
- [ ] Parenthesized amounts (98.46) as negative
- [ ] Trailing dash quantities (59-)
- [ ] Decimal precision (0.017, 0.589)

### Symbol Extraction
- [ ] Symbols before numbers preferred over symbols after
- [ ] Skip words not extracted as symbols
- [ ] No symbols for fee/tax/interest transactions
- [ ] Correct symbol with similar words (VB not CP, QQQ not SER)

### Transaction Types
- [ ] BUY vs DIVIDEND (reinvestments)
- [ ] SPLIT vs BUY (stock splits have price=0)
- [ ] FEE vs OTHER ("Journal - Expense")
- [ ] TAX vs OTHER (foreign withholding)
- [ ] TRANSFER vs DEPOSIT (account transfers)

---

## Known Limitations

### 1. Schwab Disclaimer Text
**Issue**: Phone numbers, legal text sometimes parsed as transactions
**Impact**: Minor - creates invalid entries with no symbol
**Workaround**: Manually delete or filter out entries with no symbol and $0 amount
**Status**: Needs better section detection to skip non-transaction text

### 2. Corporate Actions (Div/Int - Expense)
**Issue**: Foreign tax withholding not categorized as TAX
**Impact**: Minor - tax amounts not extracted, but net dividend correct
**Workaround**: Manual tax tracking from descriptions
**Status**: Optional enhancement, not critical

### 3. Split Detection Variations
**Issue**: Multiple split formats ("ForwardSplit", "Forward Split")
**Impact**: Some splits might be missed
**Status**: Current code handles known variations
**Recommendation**: Test with each new split event

---

## Version History

**2025-01-04 (Update 3)**: Stock split handling in parser and export
- Fixed split_ratio detection in parser (lines 1063-1080)
- Matches "stocksplit" and "forwardsplit" (with or without spaces)
- Sets split_ratio: forward_split_positive/negative, stock_split_10_for_1
- Export (dashboard.js) combines split pairs automatically
- Web UI shows ALL transactions (no combining)
- Export shows COMBINED splits (one entry per split event)
- SMCI: 2 DB entries → 1 export entry (10:1 split)
- NVDA: 2 DB entries → 1 export entry (main split only, skip fractional)

**2025-01-04 (Update 2)**: Fixed disclaimer text filtering
- Added disclaimer detection in Schwab detailed parser (lines 686-704)
- Prevents REIT/FDIC from being extracted as symbols
- Filters legal/footer text before transaction detection
- No more manual cleanup needed for disclaimer text

**2025-01-04 (Update 1)**: Added whole number quantity preference
- Fixed NVDA May 2022 issue (199.59 shares → 100 shares)
- When both qty/price pairs have equal error AND reasonable prices
- Prefer whole numbers as quantity (100 vs 199.59)
- Rationale: Stock purchases typically in whole shares, not fractional
- Dividend reinvestments remain fractional (detected by small amounts)

**2025-01-03**: Comprehensive parser fixes
- Fixed VOO quantity swap (0.589 vs balance)
- Fixed ON symbol extraction from fees
- Fixed SER/CP → QQQ/VB symbol extraction
- Fixed comma-separated thousands (CCL, MU)
- Enhanced quantity/price matching with financial heuristics
- Added FEE transaction type
- Improved line wrapping detection
- Fixed dividend reinvestment categorization
- Upgraded to pdfplumber
- Added parallel processing

**Previous versions**: See git history

---

## Git Diff Summary

### Key Changes in multi_broker_parser.py:

1. **Lines 7-17**: Added pdfplumber, ProcessPoolExecutor imports
2. **Lines 183-231**: Replaced pdftotext with pdfplumber extraction
3. **Lines 271-295**: Added FEE to negative transaction types
4. **Lines 925-929**: Skip reinvested dividend shares in Schwab parsing
5. **Lines 985-997**: Enhanced stock split detection (ForwardSplit variations)
6. **Lines 1271-1295**: Added line wrapping/continuation detection
7. **Lines 1382-1399**: Added FEE transaction type detection
8. **Lines 1467**: Excluded FEE/OTHER from symbol extraction
9. **Lines 1477-1491**: Expanded skip_words list
10. **Lines 1503-1528**: Prefer symbols before numbers
11. **Lines 1591**: Fixed comma-separated integer pattern
12. **Lines 1678-1717**: Enhanced quantity/price matching with financial heuristics
13. **Lines 2173-2257**: Added parallel processing support

---

## Contact & Maintenance

For issues or questions about parser validation:
1. Check this document first
2. Review test cases in git history
3. Examine actual PDF statements for edge cases
4. Test with small dataset before full reprocessing

**Remember**: Always backup database before reprocessing all statements!
