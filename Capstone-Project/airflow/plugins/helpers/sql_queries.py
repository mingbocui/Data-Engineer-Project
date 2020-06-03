class SqlQueries:
    
    
    # immigration_data_sample.csv
    immigrations_table_create = """CREATE TABLE IF NOT EXISTS immigrations (
                                        cicid FLOAT PRIMARY KEY,
                                        i94yr FLOAT,
                                        i94mon FLOAT,
                                        i94cit FLOAT,
                                        i94res FLOAT,
                                        i94port VARCHAR,
                                        arrdate FLOAT,
                                        i94mode FLOAT,
                                        i94addr VARCHAR,
                                        depdate FLOAT,
                                        i94bir FLOAT,
                                        i94visa FLOAT,
                                        count FLOAT,
                                        dtadfile VARCHAR,
                                        visapost VARCHAR,
                                        occup VARCHAR,
                                        entdepa VARCHAR,
                                        entdepd VARCHAR,
                                        entdepu VARCHAR,
                                        matflag VARCHAR,
                                        biryear FLOAT,
                                        dtaddto VARCHAR,
                                        gender VARCHAR,
                                        insnum VARCHAR,
                                        airline VARCHAR,
                                        admnum FLOAT,
                                        fltno VARCHAR,
                                        visatype VARCHAR
                                        );
                            """
      
    # us-cities-demographics.csv
    us_cities_demographics_table_create = """CREATE TABLE IF NOT EXISTS us_cities_demographics (
                                                city VARCHAR,
                                                state VARCHAR, 
                                                median_age FLOAT, 
                                                male_population FLOAT,
                                                female_population FLOAT,
                                                total_population FLOAT,
                                                number_of_veterans FLOAT,
                                                foreign_born FLOAT,
                                                average_household_size FLOAT,
                                                state_code VARCHAR,
                                                race VARCHAR,
                                                count INT
                                                );
                                          """
        # airport-codes_csv.csv
    airport_table_create = """CREATE TABLE IF NOT EXISTS airport (
                                        ident            VARCHAR,
                                        type             VARCHAR,
                                        name             VARCHAR,
                                        elevation_ft     FLOAT,
                                        continent        VARCHAR,
                                        iso_country      VARCHAR,
                                        iso_region       VARCHAR,
                                        municipality     VARCHAR,
                                        gps_code         VARCHAR,
                                        iata_code        VARCHAR,
                                        local_code       VARCHAR,
                                        coordinates      VARCHAR
                                        )
                                        """
    # i94visa.csv
    i94visa_table_create = """CREATE TABLE IF NOT EXISTS i94visas(
                                    code INT PRIMARY KEY,
                                    visa VARCHAR
                                    );
                                """

    # i94port.csv
    i94port_table_create = """CREATE TABLE IF NOT EXISTS i94port(
                                    code VARCHAR PRIMARY KEY,
                                    port VARCHAR
                                    );
                                """
        
    # i94mode.csv
    i94mode_table_create = """CREATE TABLE IF NOT EXISTS i94mode(
                                    code VARCHAR PRIMARY KEY,
                                    model VARCHAR
                                    );
                                """

    # i94cit.csv
    i94cit_table_create = """CREATE TABLE IF NOT EXISTS i94cit(
                                    code VARCHAR PRIMARY KEY,
                                    country VARCHAR
                                    );
                                """
        
    # i94addr.csv
    i94addr_table_create = """CREATE TABLE IF NOT EXISTS i94addr(
                                    code VARCHAR PRIMARY KEY,
                                    address VARCHAR
                                    );
                                """