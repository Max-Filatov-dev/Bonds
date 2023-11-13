from datetime import datetime, timedelta
from timeit import timeit
from time import sleep
import httpx
import json


class Bond:
    """ """

    start_dep = 1_000_000
    max_price = 1010
    min_days, max_days = 60, 730
    min_yeilds, max_yeilds = 9, 15

    def get_moex(self, url: str):
        """ """
        client = httpx.Client(http2=True)
        resp_moex = client.get(url=url)
        return (
            resp_moex.json() if resp_moex.status_code == 200 else resp_moex.status_code
        )

    def get_secid_bonds(self):
        """ """
        today, secid_dict = datetime.today().date(), {"tqob": [], "tqcb": []}
        if today.weekday() == 5:
            today -= timedelta(days=1)
        elif today.weekday() == 6:
            today -= timedelta(days=2)
        url_secid = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/yn.json?date={today}&iss.meta=off"
        resp_moex_secid = self.get_moex(url=url_secid)
        if not isinstance(resp_moex_secid, int):
            ofz = [
                secid[1]
                for secid in resp_moex_secid["yn"]["data"]
                if secid[6] == "1"
                and secid[7] == "RUB"
                and secid[8] == "Государственная облигация"
                and "RU" not in secid[1]
            ]
            corp = [
                secid[1]
                for secid in resp_moex_secid["yn"]["data"]
                if secid[6] == "1"
                and secid[7] == "RUB"
                and secid[8] != "Государственная облигация"
            ]
            secid_dict["tqob"].extend(ofz) if ofz else None
            secid_dict["tqcb"].extend(corp) if corp else None
            return secid_dict
        else:
            print(f"\nget_secid_bonds, {resp_moex_secid}")

    def get_bond_data(self, boardid: str, secid: str):
        """ """
        # yn, boards, marketdata_yields, description, marketdata
        url_bond_data = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{boardid}/securities/{secid}.json?iss.meta=off"
        bond_data = self.get_moex(url=url_bond_data)
        if (
            not isinstance(bond_data, int)
            and bond_data["marketdata"]["data"][0][11]
            and bond_data["securities"]["data"][0][15]
        ):
            nkd = bond_data["securities"]["data"][0][7]
            full_price = bond_data["marketdata"]["data"][0][11] * 10 + nkd
            max_pos = int(self.start_dep / full_price)
            nom_cup = max_pos * (1000 - (full_price - nkd))
            matdate = bond_data["securities"]["data"][0][13]
            shortname = bond_data["securities"]["data"][0][2]
            value = max_pos * full_price
            days_off = (
                datetime.strptime(matdate, "%Y-%m-%d").date() - datetime.today().date()
            ).days
            one_day = (
                bond_data["securities"]["data"][0][5]
                / bond_data["securities"]["data"][0][15]
                * max_pos
                * 0.87
            )
            cur_cup_yields = one_day * 365 / value * 100
            mat_yields = (one_day * 365 + nom_cup) / value * 100

            return {
                "secid": secid,
                "shortname": shortname,
                "full_price": round(full_price, 2),
                "cur_yields": round(cur_cup_yields, 2),
                "one_day": round(one_day, 2),
                "days_off": days_off,
                "mat_yields": round(mat_yields, 2),
                "matdate": matdate,
            }
