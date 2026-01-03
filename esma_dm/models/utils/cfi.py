"""
CFI (Classification of Financial Instruments) Code Decoder
Based on ISO 10962 Standard

This module provides complete and accurate CFI code classification
to support all FIRDS instrument types and decode any CFI code in the database.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class Category(Enum):
    """ISO 10962 Financial Instrument Categories"""

    EQUITIES = "E"  # Equities
    DEBT = "D"  # Debt instruments
    COLLECTIVE_INVESTMENT = "C"  # Collective investment vehicles (CIVs)
    ENTITLEMENTS = "R"  # Entitlements (rights)
    OPTIONS = "O"  # Options
    FUTURES = "F"  # Futures
    SWAPS = "S"  # Swaps
    NON_STANDARD = "H"  # Non-standardized derivatives
    SPOT = "I"  # Spot
    FORWARDS = "J"  # Forwards
    STRATEGIES = "K"  # Strategies
    FINANCING = "L"  # Financing
    REFERENTIAL = "T"  # Referential instruments
    OTHERS = "M"  # Others/miscellaneous


class EquityGroup(Enum):
    """Equity instrument groups (Category E)"""

    COMMON_SHARES = "S"  # Common/ordinary shares
    PREFERRED_SHARES = "P"  # Preferred/preference shares
    COMMON_CONVERTIBLE = "C"  # Common/ordinary convertible shares
    PREFERRED_CONVERTIBLE = "F"  # Preferred/preference convertible shares
    LIMITED_PARTNERSHIP = "L"  # Limited partnership units
    DEPOSITORY_RECEIPTS = "D"  # Depository receipts on equities
    STRUCTURED_PARTICIPATION = "Y"  # Structured instruments (participation)
    OTHERS = "M"  # Others (miscellaneous)


class DebtGroup(Enum):
    """Debt instrument groups (Category D)"""

    BONDS = "B"  # Bonds
    CONVERTIBLE_BONDS = "C"  # Convertible bonds
    BONDS_WITH_WARRANTS = "W"  # Bonds with warrants attached
    MEDIUM_TERM_NOTES = "T"  # Medium-term notes
    MONEY_MARKET = "Y"  # Money market instruments
    MORTGAGE_BACKED = "A"  # Mortgage backed securities
    ASSET_BACKED = "S"  # Asset backed securities
    MUNICIPAL = "N"  # Municipal securities
    DEPOSITS = "D"  # Deposits
    OTHERS = "M"  # Others (miscellaneous)


class CIVGroup(Enum):
    """Collective Investment Vehicle groups (Category C)"""

    STANDARD_FUNDS = "I"  # Standard (vanilla) investment funds/mutual funds
    HEDGE_FUNDS = "H"  # Hedge funds
    REAL_ESTATE_TRUSTS = "B"  # Real estate investment trusts (REIT)
    EXCHANGE_TRADED_FUNDS = "E"  # Exchange traded funds (ETF)
    PENSION_FUNDS = "S"  # Pension funds
    FUNDS_OF_FUNDS = "F"  # Funds of funds
    PRIVATE_EQUITY = "P"  # Private equity funds
    OTHERS = "M"  # Others (miscellaneous)


class AttributeDecoder:
    """Decode CFI attribute positions based on ISO 10962 standard"""

    # Equity Attributes
    EQUITY_VOTING_RIGHTS = {
        "V": "Voting (each share has one vote)",
        "N": "Non-voting (the shareholder has no voting right)",
        "R": "Restricted voting (the shareholder may be entitled to less than one vote per share)",
        "E": "Enhanced voting (the shareholder is entitled to more than one vote per share)",
        "X": "Not applicable/undefined",
    }

    EQUITY_OWNERSHIP_RESTRICTIONS = {
        "T": "Restrictions",
        "U": "Free (unrestricted)",
        "X": "Not applicable/undefined",
    }

    EQUITY_PAYMENT_STATUS = {
        "O": "Nil paid",
        "P": "Partly paid",
        "F": "Fully paid",
        "X": "Not applicable/undefined",
    }

    EQUITY_FORM = {
        "B": "Bearer (the owner is not registered in the books of the issuer or of the registrar)",
        "R": "Registered (securities are recorded in the name of the owner on the books of the issuer or the issuer's registrar and can only be transferred to another owner when endorsed by the registered owner)",
        "N": "Bearer/registered (securities are issued in both bearer and registered form but with the same identification number)",
        "Z": "Bearer depository receipt (Receipt - in bearer form - for securities issued in a foreign market to promote trading outside the home country of the underlying securities)",
        "M": "Others (miscellaneous)",
        "A": "Registered depository receipt (e.g. ADR, Receipt - in registered form - for securities issued in a foreign market to promote trading outside the home country of the underlying securities)",
        "X": "Not applicable/undefined",
    }

    # Preferred Share Redemption
    PREFERRED_REDEMPTION = {
        "R": "Redeemable (the shares may be redeemed at the option of the issuer and/or of the shareholder)",
        "E": "Extendible (the redemption date can be extended at the issuer or holder option)",
        "T": "Redeemable/extendible (the issuer and/or holders of redeemable shares with a fixed maturity date have the option to extend the maturity date)",
        "G": "Exchangeable (the shares may be exchanged for securities of another issuer)",
        "A": "Redeemable/exchangeable/extendible (the issuer and/or holders of redeemable shares with a fixed maturity date have the option to extend the maturity date and the shares may be exchanged for securities of another issuer)",
        "C": "Redeemable/exchangeable (the shares may be redeemed at the option of the issuer and/or of the shareholder and may be exchanged for securities of another issuer)",
        "N": "Perpetual (the share has no fixed maturity date)",
        "X": "Not applicable/undefined",
    }

    # Preferred Share Income
    PREFERRED_INCOME = {
        "F": "Fixed rate income (the shareholder periodically receives a stated income)",
        "C": "Cumulative, fixed rate income (the shareholder periodically receives a stated amount; dividends not paid in any year accumulate and shall be paid at a later date before dividends can be paid on the common/ordinary shares)",
        "P": "Participating income (preferred/preference shareholders, in addition to receiving their fixed rate of prior dividend, share with the common shareholders in further dividend distributions and in capital distributions)",
        "Q": "Cumulative, participating income (shareholders are entitled to dividends in excess of the stipulated preferential rate under specified conditions; dividends not paid in any year accumulate and shall be paid at a later date before dividends can be paid on the common/ordinary shares)",
        "A": "Adjustable/variable rate income (the dividend rate is set periodically, usually based on a certain yield)",
        "N": "Normal rate income (shareholders are entitled to the same dividends as common/ordinary shareholders, but have other privileges, for example as regards distribution of assets upon dissolution)",
        "U": "Auction rate income (dividend is adjusted through an auction, such as the Dutch auction)",
        "D": "Dividends",
        "X": "Not applicable/undefined",
    }

    # Depository Receipt Dependencies
    DEPOSITORY_DEPENDENCY = {
        "S": "Common/ordinary shares",
        "P": "Preferred/preference shares",
        "C": "Common/ordinary convertible shares",
        "F": "Preferred/preference convertible shares",
        "L": "Limited partnership units",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    # Structured Instruments Types
    STRUCTURED_TYPE = {
        "A": "Tracker certificate [participation in development of the underlying asset(s); reflects underlying price moves 1:1 (adjusted by conversion ratio and any related fees); risk is comparable to direct investment in the underlying asset(s)]",
        "B": "Outperformance certificate [participation in development of the underlying asset(s); disproportionate participation (outperformance) in positive performance above the strike; reflects underlying price moves 1:1 (adjusted by conversion ratio and any related fees); risk is comparable to direct investment in the underlying asset(s)]",
        "C": "Bonus certificate [participation in development of the underlying asset(s); minimum redemption is equal to the nominal value provided the barrier has not been breached; if the barrier is breached the product changes into a tracker certificate; with greater risk multiple underlying asset(s) (worst-of) allow for a higher bonus level or lower barrier; reduced risk compared to a direct investment into the underlying asset(s)]",
        "D": "Outperformance bonus certificate [participation in development of the underlying asset(s); disproportionate participation (outperformance) in positive performance above the strike; minimum redemption is equal to the nominal value provided the barrier has not been breached; if the barrier is breached the product changes into an outperformance certificate; with greater risk multiple underlying asset(s) (worst-of) allow for a higher bonus level or lower barrier; reduced risk compared to a direct investment into the underlying asset(s)]",
        "E": "Twin-win-certificate [participation in development of the underlying asset(s); profits possible with rising and falling underlying asset values; falling underlying asset price converts into profit up to the barrier; minimum redemption is equal to the nominal value provided the barrier has not been breached; if the barrier is breached the product changes into a tracker certificate; with higher risk levels, multiple underlying asset(s) (worst-of) allow for a higher bonus level or lower barrier; reduced risk compared to a direct investment into the underlying asset(s)]",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    # Debt Attributes
    DEBT_INTEREST_TYPE = {
        "F": "Fixed rate (all interest payments are known at issuance and remain constant for the life of the issue)",
        "Z": "Zero rate/discounted [no periodical interest payments are made; the interest charge (discount) is the difference between maturity value and proceeds at time of acquisition]",
        "V": "Variable (the interest rate is subject to adjustment through the life of the issue; includes graduated, i.e. step-up/step-down, floating and indexed interest rates)",
        "C": "Cash payment (this attribute applies only for sukuk certificates; a sukuk takes place when a set of investors pool their wealth to invest in accordance with sharia principles to earn profits which are then distributed pro rata)",
        "K": "Payment in kind (pays interest using other assets instead of cash)",
        "X": "Not applicable/undefined",
    }

    DEBT_GUARANTEE = {
        "T": "Government guarantee [the debt instrument is guaranteed by a federal, state, (semi)-government, sovereigns, agencies]",
        "G": "Joint guarantee [the debt instrument is guaranteed by an entity (e.g. corporation) other than the issuer; not a federal or state government]",
        "S": "Secured (debt issue against which specific assets are pledged to secure the obligation, e.g. mortgage or receivables)",
        "U": "Unsecured/unguaranteed (the direct obligations of the issuer rest solely on its general credit)",
        "P": "Negative pledge (the borrower agrees not to pledge any assets if such pledging would result in less security for the agreement's bondholders)",
        "N": "Senior (applies to senior debts that are placed before senior subordinated, junior and junior subordinated in the ranking in the event of liquidation)",
        "O": "Senior subordinated (applies to senior subordinated debts that are placed before junior and junior subordinated in the ranking in the event of liquidation)",
        "Q": "Junior (applies to junior debts that are placed before junior subordinated in the ranking in the event of liquidation)",
        "J": "Junior subordinated (applies to junior subordinated debts in the ranking in the event of liquidation)",
        "C": "Supranational (organization defined as being beyond the scope or borders of any one nation such as two or more central banks or two or more central governments. Examples of supranational include the United Nations, the European Union, the European Investment Bank and the World Bank)",
        "X": "Not applicable/undefined",
    }

    DEBT_REDEMPTION = {
        "F": "Fixed maturity (the principal amount is repaid in full at maturity)",
        "G": "Fixed maturity with call feature (the issue may be called for redemption prior to the fixed maturity date)",
        "C": "Fixed maturity with put feature (the holder may request the reimbursement of his bonds prior to the maturity date)",
        "D": "Fixed maturity with put and call",
        "A": "Amortization plan (reduction of principal by regular payments)",
        "B": "Amortization plan with call feature (the redemption of principal may occur as the result of the outstanding portion of the bond being called)",
        "T": "Amortization plan with put feature",
        "L": "Amortization plan with put and call",
        "P": "Perpetual (the debt instrument has no fixed maturity date and is only due for redemption in the case of the issuer's liquidation)",
        "Q": "Perpetual with call feature (the issue may be called for redemption at some time in the future)",
        "R": "Perpetual with put feature (the issue may be puttable for redemption at some time in the future)",
        "E": "Extendible",
        "X": "Not applicable/undefined",
    }

    # CIV Attributes
    CIV_CLOSED_OPEN = {
        "C": "Closed-end [units are sold on either an organized exchange or in the over-the-counter (OTC) market and are usually not redeemed]",
        "O": "Open-end (funds permanently sell new units to the public and redeem outstanding units on demand, resulting in an increase or decrease of outstanding capital)",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    CIV_DISTRIBUTION_POLICY = {
        "I": "Income funds (the fund regularly distributes its investment profits)",
        "G": "Accumulation funds (the fund normally reinvests its investment profits)",
        "J": "Mixed funds (investment profits are partly distributed, partly reinvested)",
        "X": "Not applicable/undefined",
    }

    CIV_ASSETS = {
        "R": "Real estate",
        "B": "Debt instruments (fund invests in debt instrument regardless of maturity)",
        "E": "Equities",
        "V": "Convertible securities",
        "L": "Mixed (fund invests in different assets)",
        "C": "Commodities",
        "D": "Derivatives (Fund invests in derivatives)",
        "F": "Referential instruments excluding commodities",
        "K": "Credits [contractual agreement in which a borrower receives something of value (good, service or money) now and agrees to repay the lender at some date in the future, generally with interest; CIVs normally invest in credits originated by third parties; credits are not freely transferable like debt securities]",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    CIV_SECURITY_TYPE = {
        "S": "Shares (retail and/or qualified/institutional/professional investors)",
        "Q": "Shares for QI (qualified/institutional/professional investors only)",
        "U": "Units (retail and/or qualified/institutional/professional investors)",
        "Y": "Units for QI (qualified/institutional/professional investors only)",
        "X": "Not applicable/undefined",
    }

    # Hedge Fund Strategies
    HEDGE_STRATEGY = {
        "D": "Directional [the two biggest constituents of directional are macro and commodity trading advisor (CTA)/managed futures; macro describes directional strategies that are based upon the direction of market prices of currencies, commodities, equities, fixed income and includes futures and cash markets; CTA/managed futures describe strategies that are based upon futures contracts across all asset classes only]",
        "R": "Relative value (strategies focusing on the spread relationships across various financial assets or commodities; they often utilize leverage and avoid market risk, although spread risk may often be large)",
        "S": "Security selection (strategies typically equity-based and including long/short equity; the manager attempts to make money from superior stock selection by building some combination of long and short positions in such a way to mitigate systematic market risks)",
        "E": "Event-driven (combination of investment strategies focusing on securities that are expected to experience a change in valuation due to corporate transactions or events such as bankruptcies)",
        "A": "Arbitrage (in economics and finance, arbitrage is the practice of taking advantage of a price difference between two or more markets, striking a combination of matching deals that capitalize upon the imbalance, the profit being the difference between the market prices)",
        "N": "Multi-strategy (multi-strategy as a separate set of investment strategies is broad and by it the manager is expected to maintain approximately 25% of portfolio exposure in two or more strategies that are distinct from one another)",
        "L": "Asset-based lending (strategy based on providing loans against assets to companies, including the ones viewed as not being creditworthy by commercial banks; the amount of the loan is secured by claims against the borrower's assets and as such it is directly determined by the assets' value)",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    # Futures Attributes
    FUTURES_UNDERLYING_COMMODITIES = {
        "E": "Energy (oil, gas, electricity, etc.)",
        "A": "Agricultural (grains, livestock, softs)",
        "I": "Industrial/precious metals (gold, silver, copper, etc.)",
        "S": "Index (commodity indices)",
        "N": "Environmental (carbon, weather, etc.)",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    FUTURES_UNDERLYING_FINANCIAL = {
        "B": "Debt instruments (bonds, bills, notes)",
        "S": "Equities (single stocks, indices)",
        "D": "Depository receipts",
        "C": "Currencies/foreign exchange",
        "I": "Interest rates",
        "T": "Collective investment vehicles",
        "F": "Futures (futures on futures)",
        "O": "Options",
        "W": "Swaps",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    FUTURES_DELIVERY = {
        "P": "Physical delivery (actual underlying asset delivered)",
        "C": "Cash settled (cash payment based on reference price)",
        "N": "Non-deliverable (cash settled, no physical delivery possible)",
        "X": "Not applicable/undefined",
    }

    FUTURES_STANDARDIZATION = {
        "S": "Standardized (exchange-traded with standard terms)",
        "N": "Non-standardized (customized terms)",
        "X": "Not applicable/undefined",
    }

    # Options Attributes
    OPTIONS_EXERCISE_STYLE = {
        "E": "European (can only be exercised at expiration)",
        "A": "American (can be exercised at any time before expiration)",
        "B": "Bermuda (can be exercised on specific dates)",
        "X": "Not applicable/undefined",
    }

    OPTIONS_UNDERLYING = {
        "B": "Debt instruments (bonds, bills, notes)",
        "S": "Equities (stocks, equity indices)",
        "D": "Depository receipts",
        "T": "Collective investment vehicles (ETFs, funds)",
        "C": "Currencies/foreign exchange",
        "I": "Interest rates",
        "F": "Futures contracts",
        "O": "Options (options on options)",
        "W": "Swaps",
        "A": "Commodities",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    OPTIONS_DELIVERY = {
        "P": "Physical delivery (actual underlying delivered)",
        "C": "Cash settled (cash payment)",
        "N": "Non-deliverable forward (cash settled)",
        "E": "Elect at exercise (choice at exercise time)",
        "X": "Not applicable/undefined",
    }

    # Swaps Attributes  
    SWAPS_CREDIT_UNDERLYING = {
        "U": "Single name (individual issuer)",
        "V": "Index (credit index)",
        "I": "Basket (multiple names)",
        "B": "Tranche (portfolio slice)",
        "W": "Index tranche",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    SWAPS_EQUITY_UNDERLYING = {
        "S": "Single name (individual stock)",
        "I": "Index (equity index)",
        "B": "Basket (multiple stocks)",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    SWAPS_RATES_UNDERLYING = {
        "A": "Interest rate index (LIBOR, EURIBOR, etc.)",
        "C": "Government/treasury bonds",
        "D": "Corporate bonds", 
        "G": "Mortgage-backed securities",
        "H": "Asset-backed securities",
        "I": "Municipal bonds",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    SWAPS_FX_UNDERLYING = {
        "A": "Currency (single currency pair)",
        "C": "Cross-currency (multiple currencies)",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    SWAPS_DELIVERY = {
        "C": "Cash settled",
        "P": "Physical delivery",
        "E": "Elect at settlement",
        "D": "Delivery vs payment",
        "N": "Net settlement",
        "A": "Automatic exercise",
        "X": "Not applicable/undefined",
    }

    # Forwards Attributes
    FORWARDS_EQUITY_UNDERLYING = {
        "S": "Single name (individual stock)",
        "I": "Index (equity index)",
        "B": "Basket (multiple stocks)",
        "O": "Others (equity-related)",
        "F": "Funds/ETFs",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    FORWARDS_FX_UNDERLYING = {
        "T": "Currency forward (deliverable)",
        "R": "Currency forward (non-deliverable)",
        "V": "Currency swap",
        "U": "Cross-currency swap",
        "W": "Currency option",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    FORWARDS_RATES_UNDERLYING = {
        "I": "Interest rate agreement (FRA)",
        "O": "Options on rates",
        "M": "Others (rate instruments)",
        "X": "Not applicable/undefined",
    }

    FORWARDS_PAYOUT_TRIGGER = {
        "C": "Cash settlement",
        "S": "Share/security delivery",
        "F": "Future value settlement",
        "R": "Rate differential settlement",
        "X": "Not applicable/undefined",
    }

    FORWARDS_DELIVERY = {
        "C": "Cash settled",
        "P": "Physical delivery", 
        "X": "Not applicable/undefined",
    }

    # Spot Instruments Attributes
    SPOT_COMMODITIES_UNDERLYING = {
        "A": "Agricultural products",
        "J": "Precious metals",
        "K": "Industrial metals", 
        "N": "Energy products",
        "P": "Environmental/other",
        "M": "Others (miscellaneous)",
        "X": "Not applicable/undefined",
    }

    @classmethod
    def decode_attributes(cls, cfi_code: str) -> Dict[str, Any]:
        """Decode CFI attributes based on category and group"""
        if len(cfi_code) != 6:
            return {"error": "Invalid CFI code length"}

        category = cfi_code[0]
        group = cfi_code[1]
        attrs = cfi_code[2:]

        # Equity instruments
        if category == "E":
            if group in ["S", "C"]:  # Common shares and convertible shares (both use same attributes)
                return {
                    "voting_rights": cls.EQUITY_VOTING_RIGHTS.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "ownership_restrictions": cls.EQUITY_OWNERSHIP_RESTRICTIONS.get(
                        attrs[1], f"Unknown ({attrs[1]})"
                    ),
                    "payment_status": cls.EQUITY_PAYMENT_STATUS.get(
                        attrs[2], f"Unknown ({attrs[2]})"
                    ),
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "P":  # Preferred shares
                return {
                    "voting_rights": cls.EQUITY_VOTING_RIGHTS.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "redemption": cls.PREFERRED_REDEMPTION.get(attrs[1], f"Unknown ({attrs[1]})"),
                    "income": cls.PREFERRED_INCOME.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "D":  # Depository receipts
                return {
                    "instrument_dependency": cls.DEPOSITORY_DEPENDENCY.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "redemption_conversion": cls.PREFERRED_REDEMPTION.get(
                        attrs[1], f"Unknown ({attrs[1]})"
                    ),
                    "income": cls.PREFERRED_INCOME.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "Y":  # Structured participation
                return {
                    "type": cls.STRUCTURED_TYPE.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "distribution": (
                        "Dividend payments"
                        if attrs[1] == "D"
                        else "No payments" if attrs[1] == "Y" else f"Unknown ({attrs[1]})"
                    ),
                    "repayment": (
                        "Cash repayment"
                        if attrs[2] == "F"
                        else (
                            "Physical repayment"
                            if attrs[2] == "V"
                            else (
                                "Elect at settlement"
                                if attrs[2] == "E"
                                else f"Unknown ({attrs[2]})"
                            )
                        )
                    ),
                    "underlying_assets": (
                        "Baskets"
                        if attrs[3] == "B"
                        else (
                            "Equities"
                            if attrs[3] == "S"
                            else "Debt instruments" if attrs[3] == "D" else f"Unknown ({attrs[3]})"
                        )
                    ),
                }

        # Debt instruments
        elif category == "D":
            return {
                "interest_type": cls.DEBT_INTEREST_TYPE.get(attrs[0], f"Unknown ({attrs[0]})"),
                "guarantee_ranking": cls.DEBT_GUARANTEE.get(attrs[1], f"Unknown ({attrs[1]})"),
                "redemption": cls.DEBT_REDEMPTION.get(attrs[2], f"Unknown ({attrs[2]})"),
                "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
            }

        # Collective Investment Vehicles
        elif category == "C":
            if group == "I":  # Standard funds
                return {
                    "closed_open_end": cls.CIV_CLOSED_OPEN.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "distribution_policy": cls.CIV_DISTRIBUTION_POLICY.get(
                        attrs[1], f"Unknown ({attrs[1]})"
                    ),
                    "assets": cls.CIV_ASSETS.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "security_type": cls.CIV_SECURITY_TYPE.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "H":  # Hedge funds
                return {
                    "investment_strategy": cls.HEDGE_STRATEGY.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "attribute_2": f"Not applicable ({attrs[1]})",
                    "attribute_3": f"Not applicable ({attrs[2]})",
                    "attribute_4": f"Not applicable ({attrs[3]})",
                }
            elif group in ["B", "E", "F", "P"]:  # REIT, ETF, Funds of funds, Private equity
                return {
                    "closed_open_end": cls.CIV_CLOSED_OPEN.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "distribution_policy": cls.CIV_DISTRIBUTION_POLICY.get(
                        attrs[1], f"Unknown ({attrs[1]})"
                    ),
                    "assets": (
                        cls.CIV_ASSETS.get(attrs[2], f"Unknown ({attrs[2]})")
                        if group != "B"
                        else "Real estate"
                    ),
                    "security_type": cls.CIV_SECURITY_TYPE.get(attrs[3], f"Unknown ({attrs[3]})"),
                }

        # Non-standard derivatives (H) - Real-world FIRDS patterns
        elif category == "H":
            if group == "R":  # Interest Rate Options (HRCAVC pattern)
                return {
                    "option_type": (
                        "Call Swaption" if attrs[0] == "C"
                        else "Put Swaption" if attrs[0] == "P"
                        else f"Rate Option ({attrs[0]})"
                    ),
                    "underlying_type": (
                        "Rate Index/Swap" if attrs[1] == "A"
                        else "Bond" if attrs[1] == "B"
                        else f"Rate instrument ({attrs[1]})"
                    ),
                    "exercise_style": (
                        "European" if attrs[2] == "V"
                        else "American" if attrs[2] == "A"
                        else f"Exercise ({attrs[2]})"
                    ),
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "E":  # Equity Options (HESBBC, HESEBC, etc.)
                return {
                    "underlying_type": (
                        "Single Name" if attrs[0] == "S"
                        else "Index" if attrs[0] == "I"
                        else "Basket" if attrs[0] == "B"
                        else f"Equity type ({attrs[0]})"
                    ),
                    "option_type": (
                        "Call" if attrs[1] == "B"
                        else "Put" if attrs[1] == "E"
                        else "Straddle" if attrs[1] == "S"
                        else f"Option ({attrs[1]})"
                    ),
                    "exercise_style": (
                        "American" if attrs[2] == "B"
                        else "European" if attrs[2] == "E"
                        else f"Exercise ({attrs[2]})"
                    ),
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "C":  # Credit Options/Derivatives
                return {
                    "credit_type": (
                        "Index Swaption" if attrs[0] == "I"
                        else "Single Name" if attrs[0] == "S"
                        else f"Credit ({attrs[0]})"
                    ),
                    "option_feature": f"Feature ({attrs[1]})",
                    "settlement_style": f"Settlement ({attrs[2]})",
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            else:  # Other H-category products (F, M, L, S, N, W, Y)
                return {
                    "product_feature_1": f"Feature ({attrs[0]})",
                    "product_feature_2": f"Feature ({attrs[1]})", 
                    "product_feature_3": f"Feature ({attrs[2]})",
                    "form": cls.EQUITY_FORM.get(attrs[3], f"Unknown ({attrs[3]})"),
                }

        # Futures (F category)
        elif category == "F":
            if group == "C":  # Commodities futures
                return {
                    "underlying_commodities": cls.FUTURES_UNDERLYING_COMMODITIES.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "delivery": cls.FUTURES_DELIVERY.get(attrs[1], f"Unknown ({attrs[1]})"),
                    "standardization": cls.FUTURES_STANDARDIZATION.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "not_applicable": "Not applicable/undefined" if attrs[3] == "X" else f"Unknown ({attrs[3]})",
                }
            elif group == "F":  # Financial futures
                return {
                    "underlying_financial": cls.FUTURES_UNDERLYING_FINANCIAL.get(
                        attrs[0], f"Unknown ({attrs[0]})"
                    ),
                    "delivery": cls.FUTURES_DELIVERY.get(attrs[1], f"Unknown ({attrs[1]})"),
                    "standardization": cls.FUTURES_STANDARDIZATION.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "not_applicable": "Not applicable/undefined" if attrs[3] == "X" else f"Unknown ({attrs[3]})",
                }

        # Options (O category)
        elif category == "O":
            if group in ["C", "P"]:  # Call and Put options
                option_type = "Call options" if group == "C" else "Put options"
                return {
                    "option_type": option_type,
                    "exercise_style": cls.OPTIONS_EXERCISE_STYLE.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "underlying": cls.OPTIONS_UNDERLYING.get(attrs[1], f"Unknown ({attrs[1]})"),
                    "delivery": cls.OPTIONS_DELIVERY.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "standardization": cls.FUTURES_STANDARDIZATION.get(attrs[3], f"Unknown ({attrs[3]})"),
                }

        # Swaps (S category)
        elif category == "S":
            if group == "C":  # Credit swaps
                return {
                    "underlying_credit": cls.SWAPS_CREDIT_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "return_trigger": "Credit event trigger" if attrs[1] == "C" else f"Trigger ({attrs[1]})",
                    "issuer_type": "Corporate" if attrs[2] == "C" else f"Issuer ({attrs[2]})",
                    "delivery": cls.SWAPS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "E":  # Equity swaps
                return {
                    "underlying_equity": cls.SWAPS_EQUITY_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "return_trigger": "Share price return" if attrs[1] == "S" else f"Return ({attrs[1]})",
                    "settlement_frequency": "Not applicable" if attrs[2] == "X" else f"Frequency ({attrs[2]})",
                    "delivery": cls.SWAPS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "F":  # FX swaps
                return {
                    "underlying_fx": cls.SWAPS_FX_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "currency_pair": "Not applicable" if attrs[1] == "X" else f"Pair ({attrs[1]})",
                    "settlement_method": "Not applicable" if attrs[2] == "X" else f"Method ({attrs[2]})",
                    "delivery": cls.SWAPS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "R":  # Rate swaps
                return {
                    "underlying_rate": cls.SWAPS_RATES_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "notional": "Constant notional" if attrs[1] == "C" else f"Notional ({attrs[1]})",
                    "currency": "Single currency" if attrs[2] == "S" else f"Currency ({attrs[2]})",
                    "delivery": cls.SWAPS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }

        # Forwards (J category)
        elif category == "J":
            if group == "E":  # Equity forwards
                return {
                    "underlying_equity": cls.FORWARDS_EQUITY_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "not_applicable": "Not applicable" if attrs[1] == "X" else f"Feature ({attrs[1]})",
                    "payout_trigger": cls.FORWARDS_PAYOUT_TRIGGER.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "delivery": cls.FORWARDS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "F":  # FX forwards
                return {
                    "underlying_fx": cls.FORWARDS_FX_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "not_applicable": "Not applicable" if attrs[1] == "X" else f"Feature ({attrs[1]})",
                    "payout_trigger": cls.FORWARDS_PAYOUT_TRIGGER.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "delivery": cls.FORWARDS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }
            elif group == "R":  # Rate forwards
                return {
                    "underlying_rate": cls.FORWARDS_RATES_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "not_applicable": "Not applicable" if attrs[1] == "X" else f"Feature ({attrs[1]})",
                    "payout_trigger": cls.FORWARDS_PAYOUT_TRIGGER.get(attrs[2], f"Unknown ({attrs[2]})"),
                    "delivery": cls.FORWARDS_DELIVERY.get(attrs[3], f"Unknown ({attrs[3]})"),
                }

        # Spot instruments (I category)  
        elif category == "I":
            if group == "T":  # Commodities spot
                return {
                    "underlying_commodity": cls.SPOT_COMMODITIES_UNDERLYING.get(attrs[0], f"Unknown ({attrs[0]})"),
                    "not_applicable_2": "Not applicable" if attrs[1] == "X" else f"Feature ({attrs[1]})",
                    "not_applicable_3": "Not applicable" if attrs[2] == "X" else f"Feature ({attrs[2]})",
                    "not_applicable_4": "Not applicable" if attrs[3] == "X" else f"Feature ({attrs[3]})",
                }

        # Default for unknown patterns
        return {
            "attribute_1": f"Unknown ({attrs[0]})",
            "attribute_2": f"Unknown ({attrs[1]})",
            "attribute_3": f"Unknown ({attrs[2]})",
            "attribute_4": f"Unknown ({attrs[3]})",
        }


@dataclass
class CFI:
    """CFI Code representation with comprehensive ISO 10962 decoding"""

    code: str

    def __post_init__(self):
        if len(self.code) != 6:
            raise ValueError("CFI code must be 6 characters")

    @property
    def category(self) -> str:
        """Returns the category (1st character)"""
        return self.code[0]

    @property
    def group(self) -> str:
        """Returns the group (2nd character)"""
        return self.code[1]

    @property
    def attributes(self) -> str:
        """Returns the attributes (3rd to 6th characters)"""
        return self.code[2:]

    @property
    def category_description(self) -> str:
        """Returns human readable category description"""
        descriptions = {
            "E": "Equities",
            "D": "Debt instruments",
            "C": "Collective investment vehicles (CIVs)",
            "R": "Entitlements (rights)",
            "O": "Options",
            "F": "Futures",
            "S": "Swaps",
            "H": "Non-standardized derivatives",
            "I": "Spot",
            "J": "Forwards",
            "K": "Strategies",
            "L": "Financing",
            "T": "Referential instruments",
            "M": "Others/miscellaneous",
        }
        return descriptions.get(self.category, f"Unknown category ({self.category})")

    @property
    def group_description(self) -> str:
        """Returns human readable group description"""
        # Equity groups
        if self.category == "E":
            groups = {
                "S": "Common/ordinary shares",
                "P": "Preferred/preference shares",
                "C": "Common/ordinary convertible shares",
                "F": "Preferred/preference convertible shares",
                "L": "Limited partnership units",
                "D": "Depository receipts on equities",
                "Y": "Structured instruments (participation)",
                "M": "Others (miscellaneous)",
            }
        # Debt groups
        elif self.category == "D":
            groups = {
                "B": "Bonds",
                "C": "Convertible bonds",
                "W": "Bonds with warrants attached",
                "T": "Medium-term notes",
                "Y": "Money market instruments",
                "A": "Mortgage backed securities",
                "S": "Structured instruments (capital protection)",
                "E": "Structured instruments (without capital protection)",
                "G": "Mortgage-backed securities",
                "N": "Municipal bonds",
                "D": "Depository receipts on debt instruments",
                "M": "Others (miscellaneous)",
            }
        # CIV groups
        elif self.category == "C":
            groups = {
                "I": "Standard (vanilla) investment funds/mutual funds",
                "H": "Hedge funds",
                "B": "Real estate investment trusts (REIT)",
                "E": "Exchange traded funds (ETF)",
                "S": "Pension funds",
                "F": "Funds of funds",
                "P": "Private equity funds",
                "M": "Others (miscellaneous)",
            }
        # Futures groups
        elif self.category == "F":
            groups = {
                "C": "Commodities futures",
                "F": "Financial futures",
                "M": "Others (miscellaneous)",
            }
        # Options groups
        elif self.category == "O":
            groups = {
                "C": "Call options",
                "P": "Put options", 
                "M": "Others (miscellaneous)",
            }
        # Swaps groups
        elif self.category == "S":
            groups = {
                "C": "Credit swaps",
                "E": "Equity swaps",
                "F": "Foreign exchange swaps",
                "R": "Rate swaps",
                "T": "Commodities swaps",
                "M": "Others (miscellaneous)",
            }
        # Forwards groups  
        elif self.category == "J":
            groups = {
                "E": "Equity forwards",
                "F": "Foreign exchange forwards",
                "R": "Rate forwards",
                "C": "Credit forwards",
                "T": "Commodities forwards",
                "M": "Others (miscellaneous)",
            }
        # Non-standard/Structured products groups (Real-world FIRDS patterns)
        elif self.category == "H":
            groups = {
                "R": "Interest Rate Options (Swaptions)",
                "E": "Equity Options (Single-name and Index)", 
                "C": "Credit Options/Derivatives",
                "F": "Foreign Exchange Options",
                "M": "Commodity Options/Derivatives",
                "L": "Leverage products (Warrants, Turbos)",
                "S": "Structured securities (Bonds with embedded derivatives)",
                "N": "Notes (Structured notes and certificates)",
                "W": "Warrants (Covered warrants, Barrier warrants)",
                "Y": "Others (miscellaneous structured products)",
                "T": "Commodities derivatives",
            }
        # Spot groups
        elif self.category == "I":
            groups = {
                "T": "Commodities spot",
                "F": "Foreign exchange spot",
                "M": "Others (miscellaneous)",
            }
        # Rights/Entitlements groups
        elif self.category == "R":
            groups = {
                "S": "Subscription rights",
                "P": "Purchase rights",
                "A": "Allotment (bonus) rights",
                "W": "Warrants",
                "F": "Mini-future certificates/constant leverage certificates",
                "D": "Depository receipts on entitlements",
                "M": "Others (miscellaneous)",
            }
        # Strategies groups
        elif self.category == "K":
            groups = {
                "E": "Equity strategies",
                "C": "Credit strategies",
                "F": "Foreign exchange strategies",
                "R": "Rate strategies",
                "T": "Commodities strategies",
                "Y": "Mixed assets strategies",
                "M": "Others (miscellaneous)",
            }
        # Financing groups
        elif self.category == "L":
            groups = {
                "R": "Repurchase agreements",
                "S": "Securities lending",
                "L": "Loan-lease",
                "M": "Others (miscellaneous)",
            }
        # Others groups
        elif self.category == "M":
            groups = {
                "C": "Combined instruments",
                "M": "Other assets (miscellaneous)",
            }
        # Referential instruments groups
        elif self.category == "T":
            groups = {
                "I": "Indices",
                "B": "Baskets", 
                "C": "Currencies",
                "R": "Interest rates",
                "T": "Commodities",
                "D": "Stock dividends",
                "M": "Others (miscellaneous)",
            }
        else:
            groups = {}

        return groups.get(self.group, f"Unknown group ({self.group})")

    def is_equity(self) -> bool:
        """Check if instrument is an equity"""
        return self.category == "E"

    def is_debt(self) -> bool:
        """Check if instrument is debt"""
        return self.category == "D"

    def is_collective_investment(self) -> bool:
        """Check if instrument is a collective investment vehicle"""
        return self.category == "C"

    def is_derivative(self) -> bool:
        """Check if instrument is a derivative"""
        return self.category in ["R", "O", "F", "S", "H"]

    def describe(self) -> Dict[str, Any]:
        """Returns complete description of the CFI code"""
        return {
            "cfi_code": self.code,
            "category": self.category,
            "category_description": self.category_description,
            "group": self.group,
            "group_description": self.group_description,
            "attributes": self.attributes,
            "description": self.get_description(),
            "decoded_attributes": AttributeDecoder.decode_attributes(self.code),
        }

    def get_category(self) -> str:
        """Returns the category (compatibility method)"""
        return self.category

    def get_group(self) -> str:
        """Returns the group (compatibility method)"""
        return self.group

    def get_attributes(self) -> str:
        """Returns the attributes (compatibility method)"""
        return self.attributes

    def get_description(self) -> str:
        """Returns full description (compatibility method)"""
        return f"{self.category_description} - {self.group_description}"

    def is_valid(self) -> bool:
        """Check if CFI code is valid"""
        try:
            # Basic validation - 6 characters, valid category
            if len(self.code) != 6:
                return False
            
            valid_categories = ["E", "D", "C", "R", "O", "F", "S", "H", "I", "J", "K", "L", "T", "M"]
            return self.category in valid_categories
        except Exception:
            return False

    def validate_against_iso(self) -> Dict[str, Any]:
        """
        Validate CFI code against ISO 10962 standard and return detailed validation info.
        
        Returns:
            Dictionary with validation results and suggestions
        """
        validation = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "category_valid": False,
            "group_valid": False,
            "attributes_valid": True,
            "iso_compliant": True
        }
        
        try:
            # Category validation
            valid_categories = ["E", "D", "C", "R", "O", "F", "S", "H", "I", "J", "K", "L", "T", "M"]
            if self.category not in valid_categories:
                validation["errors"].append(f"Invalid category '{self.category}'. Valid categories: {valid_categories}")
                validation["category_valid"] = False
                validation["is_valid"] = False
            else:
                validation["category_valid"] = True
            
            # Group validation based on category
            valid_groups = {
                "E": ["S", "P", "C", "F", "L", "D", "Y", "M"],
                "D": ["B", "C", "W", "T", "Y", "A", "S", "E", "G", "N", "D", "M"],
                "C": ["I", "H", "B", "E", "S", "F", "P", "M"],
                "F": ["C", "F", "M"],
                "O": ["C", "P", "M"],
                "S": ["C", "E", "F", "R", "T", "M"],
                "J": ["C", "E", "F", "R", "T", "M"],
                "H": ["C", "E", "F", "R", "T", "M"],
                "I": ["T", "F", "M"],
                "R": ["S", "P", "A", "W", "F", "D", "M"],
                "K": ["E", "C", "F", "R", "T", "Y", "M"],
                "L": ["R", "S", "L", "M"],
                "M": ["C", "M"],
                "T": ["I", "B", "C", "R", "T", "D", "M"]
            }
            
            expected_groups = valid_groups.get(self.category, [])
            if self.group not in expected_groups and expected_groups:
                validation["warnings"].append(f"Group '{self.group}' may not be standard for category '{self.category}'. Expected: {expected_groups}")
                validation["group_valid"] = False
            else:
                validation["group_valid"] = True
            
            # Attribute validation (basic - check for valid characters)
            valid_attribute_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            for i, char in enumerate(self.attributes):
                if char not in valid_attribute_chars:
                    validation["errors"].append(f"Invalid character '{char}' at position {i+3}. Must be A-Z")
                    validation["attributes_valid"] = False
                    validation["is_valid"] = False
            
            # ISO compliance check
            if not validation["category_valid"] or not validation["group_valid"]:
                validation["iso_compliant"] = False
                
        except Exception as e:
            validation["errors"].append(f"Validation error: {str(e)}")
            validation["is_valid"] = False
            validation["iso_compliant"] = False
            
        return validation


def get_attribute_labels(cfi_code: str) -> Dict[str, str]:
    """
    Get human-readable labels for CFI attributes based on category and group.
    Returns a mapping of attribute key -> display label.
    """
    if not cfi_code or len(cfi_code) < 2:
        return {}
    
    category = cfi_code[0]
    group = cfi_code[1]
    
    # Equity attributes
    if category == "E":
        if group in ["S", "C"]:  # Common shares and convertible shares
            return {
                "voting_rights": "Voting Rights",
                "ownership_restrictions": "Ownership Restrictions", 
                "payment_status": "Payment Status",
                "form": "Form"
            }
        elif group == "P":  # Preferred shares
            return {
                "voting_rights": "Voting Rights",
                "redemption": "Redemption Features",
                "income": "Income Type",
                "form": "Form"
            }
        elif group == "D":  # Depository receipts
            return {
                "instrument_dependency": "Underlying Instrument",
                "redemption_conversion": "Redemption/Conversion",
                "income": "Income Type", 
                "form": "Form"
            }
        elif group == "Y":  # Structured participation
            return {
                "type": "Structure Type",
                "distribution": "Distribution Policy",
                "repayment": "Repayment Method",
                "underlying_assets": "Underlying Assets"
            }
    
    # Debt attributes
    elif category == "D":
        return {
            "interest_type": "Interest Type",
            "guarantee_ranking": "Guarantee/Ranking",
            "redemption": "Redemption Terms",
            "form": "Form"
        }
    
    # CIV attributes
    elif category == "C":
        if group == "H":  # Hedge funds
            return {
                "investment_strategy": "Investment Strategy",
                "attribute_2": "Additional Feature 2",
                "attribute_3": "Additional Feature 3", 
                "attribute_4": "Additional Feature 4"
            }
        else:  # Other CIVs
            return {
                "closed_open_end": "Structure Type",
                "distribution_policy": "Distribution Policy",
                "assets": "Asset Focus",
                "security_type": "Security Type"
            }
    
    # Futures attributes
    elif category == "F":
        if group == "C":  # Commodities futures
            return {
                "underlying_commodities": "Underlying Commodity",
                "delivery": "Delivery Method",
                "standardization": "Standardization",
                "not_applicable": "Additional Features"
            }
        elif group == "F":  # Financial futures
            return {
                "underlying_financial": "Underlying Asset",
                "delivery": "Delivery Method",
                "standardization": "Standardization",
                "not_applicable": "Additional Features"
            }
    
    # Options attributes  
    elif category == "O":
        return {
            "option_type": "Option Type",
            "exercise_style": "Exercise Style",
            "underlying": "Underlying Asset",
            "delivery": "Delivery Method",
            "standardization": "Standardization"
        }
    
    # Swaps attributes
    elif category == "S":
        if group == "C":  # Credit swaps
            return {
                "underlying_credit": "Credit Reference",
                "return_trigger": "Return Trigger",
                "issuer_type": "Issuer Type",
                "delivery": "Settlement Method"
            }
        elif group == "E":  # Equity swaps
            return {
                "underlying_equity": "Equity Reference",
                "return_trigger": "Return Trigger", 
                "settlement_frequency": "Settlement Frequency",
                "delivery": "Settlement Method"
            }
        elif group == "F":  # FX swaps
            return {
                "underlying_fx": "Currency Pair",
                "currency_pair": "Currency Details",
                "settlement_method": "Settlement Method",
                "delivery": "Delivery Method"
            }
        elif group == "R":  # Rate swaps
            return {
                "underlying_rate": "Rate Reference",
                "notional": "Notional Type",
                "currency": "Currency Type",
                "delivery": "Settlement Method"
            }
    
    # Forwards attributes
    elif category == "J":
        if group == "E":  # Equity forwards
            return {
                "underlying_equity": "Equity Reference",
                "not_applicable": "Additional Features",
                "payout_trigger": "Payout Trigger",
                "delivery": "Settlement Method"
            }
        elif group == "F":  # FX forwards
            return {
                "underlying_fx": "Currency Reference",
                "not_applicable": "Additional Features",
                "payout_trigger": "Payout Trigger",
                "delivery": "Settlement Method"
            }
        elif group == "R":  # Rate forwards
            return {
                "underlying_rate": "Rate Reference",
                "not_applicable": "Additional Features", 
                "payout_trigger": "Payout Trigger",
                "delivery": "Settlement Method"
            }
    
    # Spot instruments
    elif category == "I":
        if group == "T":  # Commodities spot
            return {
                "underlying_commodity": "Commodity Type",
                "not_applicable_2": "Feature 2",
                "not_applicable_3": "Feature 3",
                "not_applicable_4": "Feature 4"
            }
    
    # Non-standard derivatives
    elif category == "H":
        return {
            "product_feature_1": "Product Feature 1",
            "product_feature_2": "Product Feature 2",
            "product_feature_3": "Product Feature 3", 
            "form": "Form"
        }
    
    # Default fallback
    return {
        "attribute_1": "Attribute 1",
        "attribute_2": "Attribute 2",
        "attribute_3": "Attribute 3",
        "attribute_4": "Attribute 4"
    }


def decode_cfi(cfi_code: str) -> Dict[str, Any]:
    """Convenience function to decode a CFI code"""
    try:
        cfi = CFI(cfi_code)
        result = cfi.describe()
        
        # Add attribute labels for frontend display
        result["attribute_labels"] = get_attribute_labels(cfi_code)
        
        return result
    except Exception as e:
        return {"error": str(e), "cfi_code": cfi_code}
