import requests
import pandas as pd

def create_g_map_query(business_info):
    g_map_query = [
        str(business_info["BusinessName"]),
        "in",
        str(business_info["Street"]),
        str(business_info["City"]),
        str(business_info["Zip"]),
        str(business_info["StateName"]),
        # str(business_info["country"])
    ]
    g_map_query = " ".join(g_map_query)
    return g_map_query


def parse_json(data):
    BASE_URL = "https://www.vagaro.com"
    businesses = []
    for business in data["d"]:
        business_info = {
            "BusinessName": business.get("BusinessName", "None"),
            "Street": business.get("Street", "None"),
            "City": business.get("City", "None"),
            "Zip": business.get("Zip", "None"),
            "StateName": business.get("StateName", "None"),
            "StateCode": business.get("StateCode", "None"),
            "Telephone": business.get("Telephone", "None"),
            "FacilityValue": business.get("FacilityValue", "None"),
            "VagaroURL": BASE_URL + business.get("VagaroURL", "None")
        }
        business_info["g_map_query"] = create_g_map_query(business_info)
        businesses.append(business_info)
    businesses_df = pd.DataFrame(businesses)
    return businesses_df


if __name__=='__main__':
    url = "https://www.vagaro.com/WebServices/MySampleService.asmx/PageMethodsProxyJson"

    payload = {
        "Data":"{\"currentPageIndex\":1,\"PageSize\":5000,\"objRandomBusinessData\":{\"sType\":\"all\",\"sFacilities\":\"[]\",\"sBusinessTypes\":\"\",\"sBusinessTypeIds\":\"\",\"sKidsCondition\":\"\",\"iUserID\":0,\"sBusinessIds\":\"\",\"SortType\":5,\"blnPromotion\":false,\"sCurrentRating\":0,\"sSessionIDOfUser\":\"\",\"SRandomGUID\":\"\",\"sSearchRadious\":50,\"sPromotionCodeID\":\"\",\"latlong\":\"32.71574|-117.1611\",\"CountryID\":1,\"SearchFilter\":2,\"ArrSearchFilter\":\"0,2\",\"cityName\":\"San Diego\",\"stateCode\":\"CA\",\"isFromMobile\":false,\"isCurrentLocation\":false,\"ignoreBusinessIds\":\"277153,145613,110630,42283\",\"isOnlineStore\":false,\"isOnlineGCStore\":false,\"IsDisplayCovidBadge\":false,\"isStream\":false}}","Token":"GetRandomActiveBusiness_New"
        }

    # payload = {
    #     "Data": "{\"currentPageIndex\":1,\"PageSize\":5000,\"objRandomBusinessData\":{\"sType\":\"all\",\"sFacilities\":\"[]\",\"sBusinessTypes\":\"\",\"sBusinessTypeIds\":\"[[2,73],[2,33],[2,108],[2,82],[2,24],[2,35],[2,91],[2,32]]\",\"sKidsCondition\":\"\",\"iUserID\":0,\"sBusinessIds\":\"\",\"SortType\":5,\"blnPromotion\":false,\"sCurrentRating\":0,\"sSessionIDOfUser\":\"\",\"SRandomGUID\":\"\",\"sSearchRadious\":3881,\"sPromotionCodeID\":\"\",\"latlong\":\"36.77826|-119.4179\",\"CountryID\":\"1\",\"SearchFilter\":2,\"ArrSearchFilter\":\"0,2\",\"cityName\":\"\",\"stateCode\":\"CA\",\"isFromMobile\":false,\"isCurrentLocation\":false,\"ignoreBusinessIds\":\"\",\"isOnlineStore\":false,\"isOnlineGCStore\":false,\"IsDisplayCovidBadge\":false,\"isStream\":false}}",
    #     "Token": "GetRandomActiveBusiness_New"
    # }
    headers = {
        "cookie": "vPowerV2=p1h1fshat0ihuwjlexz5gqtw; visid_incap_451694=PWB8Da%2FCRwumwV8slk%2Bpx6Tl6GUAAAAAQUIPAAAAAAC7twqDz3oILnYfYbG4JMvD; incap_ses_1670_451694=VWJ1P7jvqh4m2ouxiAgtFy9T%2BGUAAAAAssMg165nNY8VKZej8VfkJw%3D%3D; incap_ses_1131_451694=E%2FBTFhtG4BSJ4MVLvB%2ByD80P%2B2UAAAAAwNtG%2BRNK%2F4mr3cUWRlSoEQ%3D%3D; incap_ses_229_451694=vkAULciigmtTOd32d5ItA2Uc%2B2UAAAAAm97KlCO4ULakU%2BJ8mslOtQ%3D%3D",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/json; charset=UTF-8",
        "^Cookie": "visid_incap_451694=2o3eGxvyScOrFzdBx343GRp+52UAAAAAQUIPAAAAAADfbefDAhrBpEpsr937Q/PJ; _gcl_au=1.1.1365127077.1709669859; _ga=GA1.1.1661679788.1709669860; _fbp=fb.1.1709669861134.849584694; hubspotutk=b63e176e3e43580e4e723c6022aee14a; 311151=^%^22311151^%^22; __stripe_mid=52e5d110-2c4b-490c-9962-a5f0cb29a75513fbb8; 310233=^%^22310233^%^22; proximitystate_v4=^%^7B^%^22lat^%^22^%^3A^%^22^%^22^%^2C^%^22long^%^22^%^3A^%^22^%^22^%^2C^%^22countryid^%^22^%^3A^%^221^%^22^%^2C^%^22zip^%^22^%^3A^%^22^%^22^%^2C^%^22city^%^22^%^3A^%^22^%^22^%^2C^%^22state^%^22^%^3A^%^22^%^22^%^2C^%^22currencysymbol^%^22^%^3A^%^22^%^24^%^22^%^2C^%^22businesstypes^%^22^%^3A^%^22Dance^%^20Studio^%^22^%^2C^%^22service^%^22^%^3A^%^22^%^22^%^2C^%^22serviceDate^%^22^%^3A^%^22^%^22^%^2C^%^22serviceTime^%^22^%^3A^%^22^%^22^%^7D; proximityiscurrentLocation=false; 297597=297597; 186224=^%^22186224^%^22; 199989=^%^22199989^%^22; 202953=^%^22202953^%^22; __hssrc=1; vPowerV2=cjpelnvda14uo4vlvy0xpgiv; incap_ses_1131_451694=qtVaXvfYPySr9rtLvB+yD971+mUAAAAAYOboSiST3QOcm82eBD0Dfw==; cmsmaster=^%^22public^%^22; _pin_unauth=dWlkPVpERTRPREpsTVRNdFltUTVOUzAwWkRabUxUZzNOall0WkRZNE9HUm1PVFExTlRFdw; _clck=1b0n0d0^%^7C2^%^7Cfk8^%^7C0^%^7C1525; incap_ses_1134_451694=Li7zW7cCiCIOgtbpLsi8D1r9+mUAAAAAB6BEMFFkCJevX8n7f9sWyw==; CustomizableBusinessUrl=PLvXuNjhW2l9iLgThrk3em9ELbe9XK7Iwyfc2WXcttaHFzZnff6F8ysOqPh6L7Jh; incap_ses_1137_451694=Lh9SRXt2ekHvWcMpu3DHD77++mUAAAAAn0gzGPHFsoc1Bnr0e2K0xg==; CustomizableBusinessID=IAgiC9vxpaci4O/NLoGtSQ==; incap_ses_1672_451694=LFGxNChYlXa7qRr8hSM0F2cL+2UAAAAA9LqY7mbTma6LAPN2/E6IzQ==; incap_ses_1802_451694=7bNbXVJRMF39M187ff0BGXkT+2UAAAAAa44HAGOZTcCVnbYcSSvl1A==; incap_ses_1128_451694=R58/MS3YfiXh5i+oV3enD2kU+2UAAAAAoUJDUsJp4KUFiUqwWS/3SQ==; incap_ses_229_451694=zIhkdg8pjXh7c9v2d5ItA5EZ+2UAAAAAD1o0P98fkQcIIcogI9BoaA==; _ga_BGSFEW1QY1=GS1.1.1710955331.25.1.1710955369.22.0.0; amp_c57244=zfxFYQm2_kUNZgtxJdsMtI...1hpee656n.1hpee656r.5t.10.6t; _ga_6V5L0PLMBF=GS1.1.1710955331.23.1.1710955370.0.0.0; _clsk=1f7af85^%^7C1710955371912^%^7C6^%^7C0^%^7Cl.clarity.ms^%^2Fcollect; __hstc=103157407.b63e176e3e43580e4e723c6022aee14a.1709669863219.1710952423389.1710955372283.25; __hssc=103157407.1.1710955372283; proximitystate_v3=^%^7B^%^22lat^%^22^%^3A^%^2236.77826^%^22^%^2C^%^22long^%^22^%^3A^%^22-119.4179^%^22^%^2C^%^22countryid^%^22^%^3A^%^221^%^22^%^2C^%^22zip^%^22^%^3A^%^22^%^22^%^2C^%^22city^%^22^%^3A^%^22^%^22^%^2C^%^22state^%^22^%^3A^%^22CA^%^22^%^2C^%^22stateName^%^22^%^3A^%^22California^%^22^%^2C^%^22currencysymbol^%^22^%^3A^%^22^%^24^%^22^%^2C^%^22businesstypes^%^22^%^3A^%^22^%^22^%^2C^%^22service^%^22^%^3A^%^22^%^22^%^2C^%^22vagaroURL^%^22^%^3A^%^22^%^22^%^2C^%^22titleTimesTamp^%^22^%^3A^%^22^%^22^%^2C^%^22utcOffset^%^22^%^3A0^%^2C^%^22timeZoneOffSet^%^22^%^3A-8^%^2C^%^22isSupportDayLight^%^22^%^3Afalse^%^7D; _uetsid=ec6037f0e53211eebcd47f33b3c972c2; _uetvid=6ae84720db2d11eeabb891b3e7aa1887^",
        "H": "oI0g4FVbycMXvVDVR5W1ug==",
        "I": "X+hNyBy+/018OkWghHe0fw==",
        "K": "vc9/n7Yz2SjyZHq92Ikc/tl/agtYMMMA6OGhmEWaQWw=",
        "Origin": "https://www.vagaro.com",
        "Referer": "https://www.vagaro.com/listings/ca",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Val": "Tlu5ppUGbHpKJEw83sf9fTfQtLRdCN6TukJ0onQbUik=",
        "X-Requested-With": "XMLHttpRequest",
        "^sec-ch-ua": r"^\^Chromium^^;v=^\^122^^, ^\^Not",
        "sec-ch-ua-mobile": "?0",
        "^sec-ch-ua-platform": r"^\^Windows^^^",
        "token0": "",
        "token2": ""
    }

    response = requests.request("POST", url, json=payload, headers=headers)
    data = response.json()

    business_df = parse_json(data)
    business_df.to_excel('vagaro_san_diego_CA.xlsx', index=False)