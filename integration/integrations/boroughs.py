import csv
import os.path
from datetime import datetime
import json
from collections import defaultdict
import logging

from dilectic.utils import *
from dilectic.actions import *

@task
def london_borough_profiles(cfg):
    def fill_borough_profiles():
        f = open(os.path.join(cfg.raw_dir, "london-borough-profiles.csv"), encoding='Windows-1252')
        rdr = csv.reader(f)
        next(rdr)
        for row in rdr:
            v= tuple(row[1:17] + [row[38], row[41], row[45], row[48]] + [row[74]] + row[79:])
            yield v
    return db_create(cfg, 'london_borough_profiles',
        """ CREATE TABLE IF NOT EXISTS london_borough_profiles (
    --        Code VARCHAR,
            AreaName VARCHAR,
            "Inner Outer London" VARCHAR,
            "GLA Population Estimate (2015)" DOUBLE PRECISION,
            "GLA Household Estimate (2015)" DOUBLE PRECISION,
            "Inland Area Hectares" DOUBLE PRECISION,
            "Population Density Per Hectare 2015" DOUBLE PRECISION,
            "Average Age 2015" DOUBLE PRECISION,
            "Proportion Of Population Aged 0 to 15 (2015)" DOUBLE PRECISION,
            "Proportion Of Population Of Working Age (2015)" DOUBLE PRECISION,
            "Proportion Of Population Aged 65 And Over (2015)" DOUBLE PRECISION,
            "Net Internal Migration (2014)" DOUBLE PRECISION,
            "Net International Migration (2014)" DOUBLE PRECISION,
            "Net Natural Change (2014)" DOUBLE PRECISION,
            "Percentage Of Resident Population Born Abroad (2014)" DOUBLE PRECISION,
            "Largest Migrant Population By Country Of Birth (2011)" VARCHAR,
            "Percentage Of Largest Migrant Population (2011)" DOUBLE PRECISION,
    --        Second largest migrant population by country of birth (2011),
    --        % of second largest migrant population (2011),
    --        Third largest migrant population by country of birth (2011),
    --        % of third largest migrant population (2011),
    --        % of population from BAME groups (2013),
    --        % people aged 3+ whose main language is not English (2011 Census),
    --        "Overseas nationals entering the UK (NINo), (2014/15)",
    --        "New migrant (NINo) rates, (2014/15)",
    --        Largest migrant population arrived during 2014/15,
    --        Second largest migrant population arrived during 2014/15,
    --        Third largest migrant population arrived during 2014/15,
    --        EmploymentRatePercentage2014 DOUBLE PRECISION,
    --        Male employment rate (2014),
    --        Female employment rate (2014),
    --        UnemploymentRate2014 DOUBLE PRECISION,
    --        Youth Unemployment (claimant) rate 18-24 (Dec-14),
    --        Proportion of 16-18 year olds who are NEET (%) (2014),
    --        Proportion of the working-age population who claim out-of-work benefits (%) (May-2014),
    --        % working-age with a disability (2014),
    --        Proportion of working age people with no qualifications (%) 2014,
    --        Proportion of working age with degree or equivalent and above (%) 2014,
            "Gross Annual Pay, (2014)" DOUBLE PRECISION,
    --        Gross Annual Pay - Male (2014),
    --        Gross Annual Pay - Female (2014),
            "Modelled Household median income estimates 2012/13" DOUBLE PRECISION,
    --        % adults that volunteered in past 12 months (2010/11 to 2012/13),
    --        Number of jobs by workplace (2013),
    --        % of employment that is in public sector (2013),
            "Jobs Density, 2013" DOUBLE PRECISION,
    --        "Number of active businesses, 2013",
    --        Two-year business survival rates (started in 2011),
            "Crime rates per thousand population 2014/15" DOUBLE PRECISION,
    --        Fires per thousand population (2014),
    --        Ambulance incidents per hundred population (2014),
    --        "Median House Price, 2014",
    --        "Average Band D Council Tax charge (�), 2015/16",
    --        New Homes (net) 2013/14,
    --        "Homes Owned outright, (2014) %",
    --        "Being bought with mortgage or loan, (2014) %",
    --        "Rented from Local Authority or Housing Association, (2014) %",
    --        "Rented from Private landlord, (2014) %",
    --        "% of area that is Greenspace, 2005",
    --        Total carbon emissions (2013),
    --        "Household Waste Recycling Rate, 2013/14",
    --        "Number of cars, (2011 Census)",
    --        "Number of cars per household, (2011 Census)",
    --        "% of adults who cycle at least once per month, 2013/14",
    --        "Average Public Transport Accessibility score, 2014",
    --        "Achievement of 5 or more A*- C grades at GCSE or equivalent including English and Maths, 2013/14",
    --        Rates of Children Looked After (2014),
    --        % of pupils whose first language is not English (2014),
    --        % children living in out-of-work households (2014),
    --        "Male life expectancy, (2011-13)",
    --        "Female life expectancy, (2011-13)",
    --        Teenage conception rate (2013),
    --        Life satisfaction score 2011-14 (out of 10),
    --        Worthwhileness score 2011-14 (out of 10),
             "Happiness Score (2011-14) Out Of 10" DOUBLE PRECISION,
    --        Anxiety score 2011-14 (out of 10),
    --        Childhood Obesity Prevalance (%) 2013/14,
    --        People aged 17+ with diabetes (%),
    --        Mortality rate from causes considered preventable,
            "Political control in council" VARCHAR,
            "Proportion of seats won by Conservatives in 2014 election" DOUBLE PRECISION,
            "Proportion of seats won by Labour in 2014 election" DOUBLE PRECISION,
            "Proportion of seats won by Lib Dems in 2014 election" DOUBLE PRECISION,
            "Turnout At 2014 Local Elections" DOUBLE PRECISION)""",
            fill=fill_borough_profiles)
