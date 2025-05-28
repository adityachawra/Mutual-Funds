import json

def get_data():
    with open('/Users/adityachawra/Downloads/Startup/DataBase/Data/MF/investment_options-master/amfi_data_fetcher/MF_data/mutual_fund_data.json', 'r', encoding='utf-8') as file:
        return json.load(file)




def get_nav_data():
    with open('/Users/adityachawra/Downloads/Startup/DataBase/Data/MF/investment_options-master/amfi_data_fetcher/MF_data/Mutual_funds_nav.json', 'r', encoding='utf-8') as file:
        return json.load(file)

    


def get_specs_data():
    with open('/Users/adityachawra/Downloads/Startup/DataBase/Data/MF/investment_options-master/amfi_data_fetcher/MF_data/Mutual_funds_specs.json', 'r', encoding='utf-8') as file:
        return json.load(file)


if __name__ == "__main__":
    data = get_data()
    data1 = get_nav_data()
    data2 = get_specs_data()

    
    



    




