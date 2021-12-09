#!/usr/bin/python
# -*- coding: UTF-8 -*-


import sys
import getopt
import requests
import datetime


def getCoininfo(coinType):
    r = requests.post('http://sphinx-coininfo.kube-system.svc.cluster.local:50150/v1/get/coininfos')
    for info in r.json()['Infos']:
        if info['Name'] == coinType:
            return info
    return None


def createDevice():
    data = {
        'Info': {
            'Type': 'Filecoin 7302',
            'Manufacturer': 'AMD',
            'PowerComsuption': 700,
            'ShipmentAt': int(datetime.datetime(2015, 2, 21, 12, 0).timestamp())
        }
    }
    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/devices', json=data)
    for info in r.json()['Infos']:
        if data['Info']['Type'] == info['Type'] and data['Info']['Manufacturer'] == info['Manufacturer']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/device', json=data)
    return r.json()['Info']


def createVendorLocation():
    data = {
        'Info': {
            'Country': 'Japan',
            'Province': 'Tokyo',
            'City': 'Tokyo',
            'Address': 'Tokyo'
        }
    }
    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/vendor-locations')
    for info in r.json()['Infos']:
        if info['Country'] == data['Info']['Country'] and info['Province'] == data['Info']['Province'] and \
           info['City'] == data['Info']['City'] and info['Address'] == data['Info']['Address']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/vendor-location', json=data)
    return r.json()['Info']


def createPriceCurrency():
    data = {
        'Info': {
            'Name': 'USDT',
            'Unit': 'USDT',
            'Symbol': '$'
        }
    }
    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/price-currencys')
    for info in r.json()['Infos']:
        if info['Name'] == data['Info']['Name'] and info['Unit'] == data['Info']['Unit']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/price-currency', json=data)
    return r.json()['Info']


def createFeeType(feeType, feeDesc, payType):
    data = {
        'Info': {
            'FeeType': feeType,
            'FeeDescription': feeDesc,
            'PayType': payType
        }
    }
    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/fee/types')
    for info in r.json()['Infos']:
        if info['FeeType'] == data['Info']['FeeType']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/fee/type', json=data)
    return r.json()['Info']


def getAppID():
    r = requests.post('http://application-management.kube-system.svc.cluster.local:50080/v1/get/apps')
    if len(r.json()['Infos']) == 0:
        print('empty application table')
        sys.exit(1)
    return r.json()['Infos'][0]['ID']


def createFee(feeType):
    appID = getAppID()
    data = {
        'Info': {
            'AppID': appID,
            'FeeTypeID': feeType['ID'],
            'Value': 20
        }
    }
    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/fees')
    for info in r.json()['Infos']:
        if info['AppID'] == data['Info']['AppID'] and info['FeeTypeID'] == data['Info']['FeeTypeID'] and info['Value'] == data['Info']['Value']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/fee', json=data)
    return r.json()['Info']


def createGood(coinInfo, device, vendorLocation, priceCurrency, fees):
    feeIDs = []
    for fee in fees:
        feeIDs.append(fee['ID'])


    data = {
        'Info': {
            'DeviceInfoID': device['ID'],
            'SeparateFee': True,
            'UnitPower': 1,
            'DurationDays': 180,
            'CoinInfoID': coinInfo['ID'],
            'Actuals': True,
            'DeliveryAt': int(datetime.datetime(2015, 2, 21, 12, 0).timestamp()),
            'InheritFromGoodID': '00000000-0000-0000-0000-000000000000',
            'VendorLocationID': vendorLocation['ID'],
            'Price': 400,
            'BenefitType': 'platform',
            'Classic': True,
            'SupportCoinTypeIDs': [
                coinInfo['ID']
            ],
            'Total': 100000,
            'PriceCurrency': priceCurrency['ID'],
            'Title': 'Spacemesh (New)',
            'Unit': 'TiB',
            'FeeIDs': feeIDs
        }
    }

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/get/goods')
    for info in r.json()['Infos']:
        if info['CoinInfoID'] == data['Info']['CoinInfoID']:
            return info

    r = requests.post('http://cloud-hashing-goods.kube-system.svc.cluster.local:50020/v1/create/good', json=data)
    return r.json()['Info']


def createAccount(coinInfo, address):
    appID = getAppID()
    data = {
        'Info': {
            'CoinTypeID': coinInfo['ID'],
            'Address': address,
            'GeneratedBy': 'platform',
            'UsedFor': 'benefit',
            'AppID': appID,
            'UserID': '00000000-0000-0000-0000-000000000000'
        }
    }

    r = requests.post('http://cloud-hashing-billing.kube-system.svc.cluster.local:50030/v1/get/coin/accounts/by/app/user',
            json={
                'AppID': appID,
                'UserID': '00000000-0000-0000-0000-000000000000'
            })
    if 'Infos' in r.json():
        for info in r.json()['Infos']:
            if info['Address'] == data['Info']['Address']:
                return info

    r = requests.post('http://cloud-hashing-billing.kube-system.svc.cluster.local:50030/v1/create/coin/account', json=data)
    print(r.json())
    return r.json()['Info']



def createPlatformSetting(coinInfo, good):
    benefitAccount = createAccount(coinInfo, 'f1teetnxub5hprcxl2fz36s4utjuh5m3cyig55d4i')
    platformOfflineAccount = createAccount(coinInfo, 't1xujn463wtjyptmcqgeira5jrxddqu4ny5nhpwcq')
    userOnlineAccount = createAccount(coinInfo, 't14axq5govqaym6wbc37brrfibo34sok3rgbp2jjq')
    userOfflineAccount = createAccount(coinInfo, 't14axq5govqaym6wbc37brrfibo34sok3rgbp2jjq')

    data = {
        'Info': {
            'GoodID': good['ID'],
            'BenefitAccountID': benefitAccount['ID'],
            'PlatformOfflineAccountID': platformOfflineAccount['ID'],
            'UserOnlineAccountID': userOnlineAccount['ID'],
            'UserOfflineAccountID': userOfflineAccount['ID'],
            'BenefitIntervalHours': 24,
        }
    }
    r = requests.post('http://cloud-hashing-billing.kube-system.svc.cluster.local:50030/v1/get/platform/setting/by/good',
            json={
                'GoodID': good['ID'],
            })
    if 'Info' in r.json():
        for info in r.json()['Info']:
            print(info)
            return info

    r = requests.post('http://cloud-hashing-billing.kube-system.svc.cluster.local:50030/v1/create/platform/setting', json=data)
    return r.json()['Info']


def createOrder(good, appID, userID, units):
    now = int(datetime.datetime.now().timestamp())

    data = {
        'Info':{
            'GoodID': good['ID'],
            'AppID': appID,
            'UserID': userID,
            'Units': units,
            'Start': now,
            'End': now + good['DurationDays'] * 24 * 60 * 60
        }
    }
    r = requests.post('http://cloud-hashing-order.kube-system.svc.cluster.local:50040/v1/create/order', json=data)
    return r.json()['Info']


def createConfirmedPayment(orderID, accountID, coinInfoID):
    data = {
        'Info':{
            'OrderID': orderID,
            'AccountID': accountID,
            "StartAmount": 1.2,
            "Amount": 2.3,
            "CoinInfoID": coinInfoID,
            "State": "done",
            "ChainTransactionID": "",
            "PlatformTransactionID": "00000000-0000-0000-0000-000000000000"
        }
    }
    r = requests.post('http://cloud-hashing-order.kube-system.svc.cluster.local:50040/v1/create/payment', json=data)
    r = requests.post('http://cloud-hashing-order.kube-system.svc.cluster.local:50040/v1/update/payment', json=data)
    return r.json()['Info']


def getAppID():
    r = requests.post('http://application-management.kube-system.svc.cluster.local:50080/v1/get/apps')
    if len(r.json()['Infos']) == 0:
        print('empty application table')
        sys.exit(1)
    return r.json()['Infos'][0]['ID']


def getAllUsers():
    r = requests.post("http://application-management.kube-system.svc.cluster.local:50080/v1/get/users/from/app",
    json={
        'AppID': getAppID()
    })
    appUsers =  r.json()['Infos']

    users = []

    r = requests.post("http://user-management.kube-system.svc.cluster.local:50070/v1/get/users")
    for user in appUsers:
        for info in r.json()['Infos']:
            if info['UserID'] == user['UserID']:
                users.append(info)

    return users


class Good:
    def __init__(self, coinType):
        self.coinType = coinType

    def create(self):
        coinInfo = getCoininfo(self.coinType)
        if coinInfo is None:
            print('fail get coin info {}' . format(self.coinType))
            sys.exit(1)

        device = createDevice()
        if device is None:
            print('fail create device info')
            sys.exit(2)

        vendorLocation = createVendorLocation()
        if vendorLocation is None:
            print('fail create vendor location')
            sys.exit(3)

        priceCurrency = createPriceCurrency()
        if priceCurrency is None:
            print('fail create price currency')
            sys.exit(4)

        feeType1 = createFeeType('Maintenance Fee', 'Maintenance Fee', 'percent')
        if feeType1 is None:
            print('fail create fee type1')
            sys.exit(5)

        fee1 = createFee(feeType1)
        if fee1 is None:
            print('fail create fee1')
            sys.exit(6)

        feeType2 = createFeeType('Technique Service Fee', 'Technique Service Fee', 'amount')
        if feeType2 is None:
            print('fail create fee type2')
            sys.exit(5)

        fee2 = createFee(feeType2)
        if fee2 is None:
            print('fail create fee2')
            sys.exit(6)

        good = createGood(coinInfo, device, vendorLocation, priceCurrency, [fee1, fee2])
        if good is None:
            print('fail create good')
            sys.exit(7)

        platformSetting = createPlatformSetting(coinInfo, good)
        if platformSetting is None:
            print('fail create platform setting')
            sys.exit(8)

        print('Success create good {}' . format(good))

        users = getAllUsers()
        if users is None or len(users) == 0:
            print("fail get all users")
            sys.exit(9)

        order1 = createOrder(good, getAppID(), users[0]['UserID'], 10)
        if order1 is None:
            print("fail create order1")
            sys.exit(10)

        print('Success create order1 {}' . format(order1))

        order2 = createOrder(good, getAppID(), users[1]['UserID'], 12)
        if order2 is None:
            print("fail create order2")
            sys.exit(11)

        print('Success create order2 {}' . format(order2))

        payment1 = createConfirmedPayment(order1['ID'], "00000000-0000-0000-0000-000000000000", coinInfo["ID"]) 
        if payment1 is None:
            print("fail create payment1")
            sys.exit(12)

def main(argv):
    opts, args = getopt.getopt(argv, 'c:', ['cointype='])

    coinType = ''

    for opt, arg in opts:
        if opt in ('-c', '--cointype'):
            coinType = arg

    if len(coinType) == 0:
        sys.exit(2)

    good = Good(coinType)
    good.create()


if __name__ == '__main__':
    main(sys.argv[1:])
