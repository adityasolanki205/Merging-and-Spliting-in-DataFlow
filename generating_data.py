#!/usr/bin/env python
# coding: utf-8

import random

LINE ="""{Existing_account} {Duration_month} {Credit_history} {Purpose} {Credit_amount} {Saving} {Employment_duration} {Installment_rate} {Personal_status} {Debtors} {Residential_Duration} {Property} {Age} {Installment_plans} {Housing} {Number_of_credits} {Job} {Liable_People} {Telephone} {Foreign_worker} {Classification}"""
def generate_log():
    existing_account = ['B11','A12','C14',
                        'D11','E11','A14',
                        'G12','F12','A11',
                        'NULL','H11','I11',
                        'J14','K14','L11',
                        'A13'
                       ]
    Existing_account = random.choice(existing_account)
    
    duration_month = []
    for i  in range(6, 90 , 3):
        duration_month.append(i)
    Duration_month = random.choice(duration_month)
    
    credit_history = [
                    'A34',
                    'A32',
                    'A33',
                    'A30',
                    'A31'
                    ]
    Credit_history = random.choice(credit_history)
    
    purpose = [ 'A43','NULL','A42',
                'A40','A46','A41',
                'A49','A44','A45',
                'A410','A48'
    ]
    Purpose = random.choice(purpose)
    
    credit_amount = []
    for i in range(0 ,20000):
        credit_amount.append(i)
    Credit_amount = random.choice(credit_amount)
    
    saving = [
            'A65','A61',
            'A63','A64',
            'A62']
    Saving = random.choice(saving)
    
    employment_duration = [
                        'A75',
                        'A73',
                        'A74',
                        'A71',
                        'A72']
    Employment_duration = random.choice(employment_duration)
    
    installment_rate = ['4',
                        '2',
                        '3',
                        '1'
    ]
    Installment_rate = random.choice(installment_rate)
    
    personal_status = ['A93',
    'A92',
    'A91',
    'A94',
    'NULL'
    ]
    Personal_status = random.choice(personal_status)
    
    debtors = ['A101',
    'A103',
    'A102'
    ]
    Debtors = random.choice(debtors)
    
    residential_Duration = ['4',
    '2',
    '3',
    '1'
    ]
    Residential_Duration = random.choice(residential_Duration)
    
    Proprty = ['A121',
    'A122',
    'A124',
    'A123',
    'NULL'
    ]
    Property = random.choice(Proprty)
    
    age = []
    for i in range(20 , 60):
        age.append(i)
    Age = random.choice(age)
    
    installment_plans = ['A143',
                        'A141',
                        'A142',
                        'NULL'
    ]
    Installment_plans = random.choice(installment_plans)
    
    housing = ['A152',
    'A153',
    'A151'
    ]
    Housing = random.choice(housing)
    
    number_of_credits = []
    for i in range(1,3):
        number_of_credits.append(i)
    Number_of_credits = random.choice(number_of_credits)
    
    job = ['A173',
    'A172',
    'A174',
    'A171']
    Job = random.choice(job)
    
    liable_People = ['1',
                    '2']
    Liable_People = random.choice(liable_People)
    
    telephone = ['A192',
    'A191',
    ]
    Telephone = random.choice(telephone)
    
    foreign_worker = ['A201',
    'A202']
    Foreign_worker = random.choice(foreign_worker)
    
    classification = ['NULL',
    '1',
    '2']
    Classification = random.choice(classification)
    log_line = LINE.format(
        Existing_account=Existing_account,
        Duration_month=Duration_month,
        Credit_history=Credit_history,
        Purpose=Purpose,
        Credit_amount=Credit_amount,
        Saving=Saving,
        Employment_duration=Employment_duration,
        Installment_rate=Installment_rate,
        Personal_status=Personal_status,
        Debtors=Debtors,
        Residential_Duration=Residential_Duration,
        Property=Property,
        Age=Age,
        Installment_plans=Installment_plans,
        Housing=Housing,
        Number_of_credits = Number_of_credits,
        Job= Job,
        Liable_People=Liable_People,
        Telephone=Telephone,
        Foreign_worker=Foreign_worker,
        Classification=Classification
    )

    return log_line
