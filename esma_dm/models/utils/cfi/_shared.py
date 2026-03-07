"""
Shared attribute dictionaries used across multiple CFI category modules.
All values sourced from ISO 10962 (Classification of Financial Instruments).
"""

# ---------------------------------------------------------------------------
# Form (bearer / registered)
# ---------------------------------------------------------------------------

FORM = {
    "B": "Bearer (the owner is not registered in the books of the issuer or of the registrar)",
    "R": "Registered (securities are recorded in the name of the owner on the books of the issuer "
         "or the issuer's registrar and can only be transferred to another owner when endorsed by "
         "the registered owner)",
    "N": "Bearer/registered (securities are issued in both bearer and registered form but with the "
         "same identification number)",
    "Z": "Bearer depository receipt (receipt in bearer form for securities issued in a foreign "
         "market to promote trading outside the home country of the underlying securities)",
    "A": "Registered depository receipt (e.g. ADR; receipt in registered form for securities "
         "issued in a foreign market to promote trading outside the home country of the underlying "
         "securities)",
    "M": "Others (miscellaneous)",
}

# ---------------------------------------------------------------------------
# Delivery variants
# ---------------------------------------------------------------------------

DELIVERY_CP = {
    "C": "Cash (the discharge of an obligation by payment or receipt of a net cash amount instead "
         "of payment or delivery by both parties)",
    "P": "Physical (the meeting of a settlement obligation through the receipt or delivery of the "
         "actual underlying instrument(s) instead of through cash settlement)",
}

DELIVERY_CPE = {
    "C": "Cash (the discharge of an obligation by payment or receipt of a net cash amount instead "
         "of payment or delivery by both parties)",
    "P": "Physical (the meeting of a settlement obligation through the receipt or delivery of the "
         "actual underlying instrument(s) instead of through cash settlement)",
    "E": "Elect at exercise (the method of delivery of the underlying instrument when the option "
         "is exercised shall be determined at the time of exercise)",
}

DELIVERY_CPNE = {
    "C": "Cash (the discharge of an obligation by payment or receipt of a net cash amount instead "
         "of payment or delivery by both parties)",
    "P": "Physical (the meeting of a settlement obligation through the receipt or delivery of the "
         "actual underlying instrument(s) instead of through cash settlement)",
    "N": "Non-deliverable (synthetic instruments based on non-convertible or thinly traded "
         "currencies where settlement occurs in a freely convertible currency)",
    "E": "Elect at exercise (the method of delivery of the underlying instrument when the option "
         "is exercised shall be determined at the time of exercise)",
}

DELIVERY_REPO = {
    "D": "Delivery versus payment (the borrower delivers the collateral to the lender against "
         "payment of funds within a securities settlement system)",
    "H": "Hold-in-custody (the borrower holds the collateral in a segregated customer account, "
         "in custody, for the lender)",
    "T": "Tri-party (the borrower delivers the collateral to the lender's account at the lender's "
         "clearing bank or custodian)",
}

# ---------------------------------------------------------------------------
# Exercise option style (E/A/B used in listed options and warrants)
# ---------------------------------------------------------------------------

EXERCISE_STYLE_EAB = {
    "E": "European (can only be exercised for a short, specified period of time just prior to "
         "its expiration, usually a single day)",
    "A": "American (can be exercised at any time between the purchase date and the expiration date)",
    "B": "Bermudan (can only be exercised on predetermined dates, usually every month)",
    "M": "Others (miscellaneous)",
}

# ---------------------------------------------------------------------------
# Option style and type (A-I) used in Category H (non-listed / complex)
# ---------------------------------------------------------------------------

OPTION_STYLE_TYPE = {
    "A": "European-Call (holder may exercise the right to buy at a fixed price only on expiration date)",
    "B": "American-Call (holder may exercise the right to buy at any time up to and including expiration)",
    "C": "Bermudan-Call (holder may exercise the right to buy on specific dates within the exercise period)",
    "D": "European-Put (holder may exercise the right to sell at a fixed price only on expiration date)",
    "E": "American-Put (holder may exercise the right to sell at any time up to and including expiration)",
    "F": "Bermudan-Put (holder may exercise the right to sell on specific dates within the exercise period)",
    "G": "European-Chooser (holder may choose call or put only on the contract's expiration date)",
    "H": "American-Chooser (holder may choose call or put at any time up to and including expiration)",
    "I": "Bermudan-Chooser (holder may choose call or put on specific dates within the exercise period)",
    # FX-specific option styles
    "J": "European (holder may exercise the right to buy/sell only on the expiration date)",
    "K": "American (holder may exercise the right to buy/sell at any time up to expiration)",
    "L": "Bermudan (holder may exercise the right to buy/sell on specific dates within the exercise period)",
}

# ---------------------------------------------------------------------------
# Valuation method / trigger (Category H)
# ---------------------------------------------------------------------------

VALUATION_METHOD = {
    "V": "Vanilla (an option for which all terms are standardized)",
    "A": "Asian (either the strike price or settlement price is the average level of an underlying "
         "instrument over a predetermined period)",
    "D": "Digital/Binary (a pre-determined payout if the option is in-the-money and the payoff "
         "condition is satisfied; also referred to as a binary or all-or-nothing option)",
    "B": "Barrier (final exercise depends upon the path taken by the price of the underlying; "
         "knock-out cancelled if underlying crosses barrier, knock-in becomes exercisable if "
         "underlying crosses barrier)",
    "G": "Digital barrier (a digital option embedded with a barrier option)",
    "L": "Lookback (minimizes uncertainties related to the timing of market entry; fixed or "
         "floating strike determined at purchase or maturity)",
    "P": "Other path dependent (payoff is directly related to the price pattern the underlying "
         "asset follows during the life of the contract)",
    "C": "Cap (payment triggered when the value of the underlier exceeds a specified level)",
    "F": "Floor (payment triggered when the value of the underlier falls below a specified level)",
    "M": "Others (miscellaneous)",
}

# ---------------------------------------------------------------------------
# Debt instrument shared attribute dicts
# ---------------------------------------------------------------------------

DEBT_INTEREST_TYPE = {
    "F": "Fixed rate (all interest payments are known at issuance and remain constant for the "
         "life of the issue)",
    "Z": "Zero rate/discounted (no periodic interest payments; the interest charge is the "
         "difference between maturity value and proceeds at time of acquisition)",
    "V": "Variable (the interest rate is subject to adjustment through the life of the issue; "
         "includes graduated step-up/step-down, floating and indexed interest rates)",
    "C": "Cash payment (applies only for sukuk certificates; profits are distributed pro rata "
         "among investors in accordance with sharia principles)",
    "K": "Payment in kind (pays interest using other assets instead of cash)",
}

DEBT_GUARANTEE = {
    "T": "Government guarantee (guaranteed by a federal, state, semi-government, sovereign or agency)",
    "G": "Joint guarantee (guaranteed by an entity other than the issuer, not a government)",
    "S": "Secured (specific assets are pledged to secure the obligation, e.g. mortgage or receivables)",
    "U": "Unsecured/unguaranteed (direct obligations of the issuer rest solely on its general credit)",
    "P": "Negative pledge (borrower agrees not to pledge any assets that would result in less "
         "security for the agreement's bondholders)",
    "N": "Senior (placed before senior subordinated, junior and junior subordinated in liquidation ranking)",
    "O": "Senior subordinated (placed before junior and junior subordinated in liquidation ranking)",
    "Q": "Junior (placed before junior subordinated in liquidation ranking)",
    "J": "Junior subordinated (lowest ranking in the event of liquidation)",
    "C": "Supranational (organization beyond the scope or borders of any one nation, e.g. UN, EU, "
         "European Investment Bank, World Bank)",
}

DEBT_REDEMPTION = {
    "F": "Fixed maturity (the principal amount is repaid in full at maturity)",
    "G": "Fixed maturity with call feature (the issue may be called for redemption prior to fixed maturity)",
    "C": "Fixed maturity with put feature (the holder may request reimbursement prior to maturity)",
    "D": "Fixed maturity with put and call",
    "A": "Amortization plan (reduction of principal by regular payments)",
    "B": "Amortization plan with call feature (redemption of principal may occur as the result of "
         "the outstanding portion of the bond being called)",
    "T": "Amortization plan with put feature",
    "L": "Amortization plan with put and call",
    "P": "Perpetual (no fixed maturity date; only due for redemption in the case of the issuer's liquidation)",
    "Q": "Perpetual with call feature (may be called for redemption at some time in the future)",
    "R": "Perpetual with put feature (may be puttable for redemption at some time in the future)",
    "E": "Extendible",
}

# ---------------------------------------------------------------------------
# Standardization (used in Futures and Options)
# ---------------------------------------------------------------------------

STANDARDIZATION = {
    "S": "Standardized (the underlying instruments, exercise price, expiration date and contract "
         "size of the option/future are standardized)",
    "N": "Non-standardized (have non-standard delivery or expiry terms)",
}

# ---------------------------------------------------------------------------
# Payout / return trigger (Forwards, Spots)
# ---------------------------------------------------------------------------

PAYOUT_TRIGGER = {
    "C": "CFD (contract for difference; a cash-settled total return swap or forward where the "
         "parties agree to exchange the difference in value of an underlying asset)",
    "S": "Spread-bet (the payout is determined by the movement in the reference price of the "
         "underlying instrument to its price at expiry multiplied by an agreed amount per point movement)",
    "F": "Forward price of underlying instrument (the agreed-upon price for the time of delivery)",
    "R": "Rolling spot (an indefinitely renewed position in which no currency is actually delivered "
         "until a party closes out its position)",
}
