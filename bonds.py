from datetime import datetime, timedelta
from timeit import timeit
from time import sleep
import httpx
import json


class Bond:
    """ """

    client = httpx.Client(http2=True)

    # bonds_secid_rub = {'ofz': ('TQOB', 'RU000A105BC7', 'SU26233RMFS5'), 'corp': ('TQCB', 'RU000A0JUW31', 'RU000A0JV243')}

    start_dep = 1_000_000
    max_price = 1010
    min_days, max_days = 60, 730
    min_yeilds, max_yeilds = 9, 15

    def get_moex_bond(self, url: str):
        """ """
        get_moex = self.client.get(url=url)
        return get_moex.json() if get_moex.status_code == 200 else get_moex.status_code

    def get_secid_bonds(self):
        """ """
        today = datetime.today().date()
        if today.weekday() == 5:
            today -= timedelta(days=1)
        elif today.weekday() == 6:
            today -= timedelta(days=2)
        url_bonds = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/yn.json?date={today}"
        # get_moex_bonds = self.client.get(url=url_bonds)
        # res_moex_bonds = get_moex_bonds.json()
        res_moex_bonds = self.get_moex_bond(url=url_bonds)
        # if res_moex_bonds["yn"]["data"]:
        if not isinstance(res_moex_bonds, int):
            secid_list = [secid[1] for secid in res_moex_bonds["yn"]["data"]]
            # self.check_type_bond(check_list=secid_list)
            return secid_list
        else:
            print(f"\nget_secid_bonds, {res_moex_bonds}")

    def check_type_bond(self, check_list: list):
        """ """
        # 'Государственная облигация', 'Структурные облигации'
        total, ofz, corp, usd, eur = {}, ["TQOB"], ["TQCB"], [], []
        for secid in check_list:
            url_data = f"https://iss.moex.com/iss/securities/{secid}.json"
            response_type = self.client.get(url=url_data)
            bond_data = response_type.json()
            if bond_data["description"]["data"]:
                item = [
                    (tp[2], cr[2], int(lv[2]))
                    for tp in bond_data["description"]["data"]
                    if tp[0] == "TYPENAME"
                    for cr in bond_data["description"]["data"]
                    if cr[0] == "FACEUNIT"
                    for lv in bond_data["description"]["data"]
                    if lv[0] == "LISTLEVEL"
                ]
                if (
                    item
                    and item[0][0] == "Государственная облигация"
                    and item[0][1] == "SUR"
                ):
                    ofz.append(bond_data["description"]["data"][0][2])
                elif (
                    item
                    and item[0][0] != "Структурные облигации"
                    and item[0][1] == "SUR"
                    and 1 <= item[0][2] < 3
                ):
                    corp.append(bond_data["description"]["data"][0][2])
                elif item and item[0][1] == "USD" and 1 <= item[0][2] < 3:
                    usd.append(bond_data["description"]["data"][0][2])
                elif item and item[0][1] == "EUR" and 1 <= item[0][2] < 3:
                    eur.append(bond_data["description"]["data"][0][2])

            # count += 1
            # if count % 300 == 0:
            #     sleep(10)

        total["ofz"] = ofz if ofz else None
        total["corp"] = corp if corp else None
        total["usd"] = usd if usd else None
        total["eur"] = eur if eur else None

        with open("json/bonds/bonds_secid.json", "w") as sec:
            json.dump(total, sec, indent=4)

    def get_bond_data(self, secid: str):
        """ """
        pass
        # columns = bonds_data['securities'].get('columns')
        # data = pd.Series(data=bonds_data['securities'].get('data')[0], index=columns)

    def get_bonds_data(self, boardid: str, secid: str):
        """ """
        # yn, boards, marketdata_yields, description, marketdata
        url_data = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{boardid}/securities/{secid}.json"
        response_moex = self.client.get(url=url_data)
        bonds_data = response_moex.json()

        # columns = [col for col in bonds_data['securities']['columns']]
        # data = [dt for dt in bonds_data['securities']['data'][0]]
        # for i, j in zip(columns, data):
        #      print(f"{i:<25}{j}")

        if bonds_data["securities"]["data"]:
            for val in bonds_data["securities"]["data"]:
                days_off = (
                    (
                        datetime.strptime(val[13], "%Y-%m-%d").date()
                        - datetime.today().date()
                    ).days
                    if val[13] != "0000-00-00"
                    else None
                )
                if days_off and days_off >= 60:
                    return bonds_data["securities"]["data"][0]

    def coupon_value(self, coup_date: str, nkd: float, period: int):
        """ """
        # coup_date=cur[6], nkd=cur[7], period=cur[15]
        next_pay_days = (
            datetime.strptime(coup_date, "%Y-%m-%d").date() - datetime.today().date()
        ).days
        return nkd / (period - next_pay_days) * period

    def get_current_bonds(self, cur_data: list):
        """ """
        full_coupon_cur = (
            self.coupon_value(
                coup_date=cur_data[6], nkd=cur_data[7], period=cur_data[15]
            )
            if cur_data[5] == 0 and cur_data[7] != 0
            else cur_data[5]
        )
        cur_price_nkd = cur_data[8] * 10 + cur_data[7] if cur_data[8] else None
        nkd_perc = (
            round(cur_data[7] / full_coupon_cur * 100, 2)
            if full_coupon_cur != 0
            else None
        )
        days_off_cur = (
            datetime.strptime(cur_data[13], "%Y-%m-%d").date() - datetime.today().date()
        ).days

        if (
            nkd_perc
            and cur_price_nkd
            and days_off_cur
            and nkd_perc <= 20
            and cur_price_nkd <= self.max_price
            and self.min_days <= days_off_cur <= self.max_days
        ):
            return (
                cur_data[0],
                cur_data[2],
                round(cur_price_nkd, 2),
                nkd_perc,
                round(full_coupon_cur, 2),
                cur_data[15],
                days_off_cur,
            )

    def get_next_bonds(self, nx_data: dict):
        """ """
        full_coupon_next = (
            self.coupon_value(coup_date=nx_data[6], nkd=nx_data[7], period=nx_data[15])
            if nx_data[5] == 0 and nx_data[7] != 0
            else nx_data[5]
        )
        next_price = nx_data[8] * 10 if nx_data[8] else None
        days_next_coupon = (
            (
                datetime.strptime(nx_data[6], "%Y-%m-%d").date()
                - datetime.today().date()
            ).days
            if nx_data[6] != "0000-00-00"
            else None
        )
        days_off_nx = (
            datetime.strptime(nx_data[13], "%Y-%m-%d").date() - datetime.today().date()
        ).days

        if (
            next_price
            and days_next_coupon
            and days_next_coupon <= 30
            and next_price <= self.max_price
            and self.min_days <= days_off_nx <= self.max_days
        ):
            return (
                nx_data[0],
                nx_data[2],
                round(next_price, 2),
                days_next_coupon,
                round(full_coupon_next, 2),
                nx_data[15],
                days_off_nx,
            )

    def bond_yields(self, yields_data: list):
        """ """
        temp_list = []
        for bond in yields_data[1:]:
            one_day = bond[4] / bond[5]
            total_bonds = int(self.start_dep / bond[2])
            profit_one_day = total_bonds * one_day
            profit_30_days = profit_one_day * 30
            profit_365_days = profit_one_day * 365
            profit_365_perc = profit_365_days / self.start_dep * 100
            temp_list.append(
                (
                    yields_data[0],
                    bond[0],
                    bond[1],
                    total_bonds,
                    round(profit_one_day, 2),
                    round(profit_30_days, 2),
                    round(profit_365_days, 2),
                    round(profit_365_perc, 2),
                    bond[6],
                )
            ) if self.min_yeilds <= profit_365_perc < self.max_yeilds else None
        if temp_list:
            temp_list.sort(reverse=True, key=lambda element: element[7])
            # print_result(option="yields", print_data=temp_list)

    def write_bonds_data(self):
        """ """
        count, update_bonds = 0, []
        with open("json/bonds/bonds_secid.json") as bd:
            bd_secid = json.load(bd)

        for key, val in bd_secid.items():
            if key == "ofz" or key == "corp":
                board = val[0]
                for secid in val[1:]:
                    response_data = self.get_bonds_data(boardid=board, secid=secid)
                    update_bonds.append(response_data) if response_data else None

                    count += 1
                    if count % 200 == 0:
                        # print('------ sleep ------')
                        sleep(10)

        if update_bonds:
            update_bonds.append(datetime.today().strftime("%Y-%m-%d %H:%M"))
            with open("json/bonds/cur_bonds.json", "w", encoding="utf-8") as dt:
                json.dump(update_bonds, dt, indent=4, ensure_ascii=False)

    def open_bonds_data(self):
        """ """
        cur_bonds, next_bonds = [], []
        with open("json/bonds/cur_bonds.json") as rd:
            bond_data = json.load(rd)

        dt = (
            datetime.today().date()
            - datetime.strptime(bond_data[-1], "%Y-%m-%d %H:%M").date()
        )
        if dt.days >= 130:
            print(f"\nData needs to be updated! {dt}\n")
        else:
            for item in bond_data[:-1]:
                # if item[0] == 'RU000A1027M8':
                #     print(item)
                response_cur_bond = self.get_current_bonds(cur_data=item)
                # print(response_cur_bond) if response_cur_bond else None
                cur_bonds.append(response_cur_bond) if response_cur_bond else None
                response_next_bond = self.get_next_bonds(nx_data=item)
                # print(response_next_bond) if response_next_bond else None
                next_bonds.append(response_next_bond) if response_next_bond else None

        if cur_bonds:
            # cur_bonds[0] = "Current"
            # print_result(option='cur', print_data=cur_bonds)
            name = {"name": [name for al in cur_bonds for name in al]}
            # bond_yields(yields_data=cur_bonds)
        else:
            print(f"Len current_bonds: {len(cur_bonds)}")

        if next_bonds:
            # next_bonds[0] = "Next"
            # print_result(option='next', print_data=next_bonds)
            self.bond_yields(yields_data=next_bonds)
        else:
            print(f"Len next_bonds: {len(next_bonds)}")


# if __name__ == "__main__":
#     # print(
#     #     f"\n{'=' * 50}\nTotal time: {timedelta(seconds=int(timeit(stmt=get_secid_bonds, number=1)))}\n"
#     # )
#     # print(
#     #     f"\n{'=' * 50}\nTotal time: {timedelta(seconds=int(timeit(stmt=write_bonds_data, number=1)))}\n"
#     # )
#     print(
#         f"\n{'=' * 50}\nTotal time: {timedelta(seconds=int(timeit(stmt=open_bonds_data, number=1)))}\n"
#     )
