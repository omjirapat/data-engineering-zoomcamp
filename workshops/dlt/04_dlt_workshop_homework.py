import dlt

def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

if __name__ == "__main__":
    
    pipeline = dlt.pipeline(pipeline_name="people_pipeline",
                            destination="duckdb",
                            dataset_name="all_people")
    print("insert the first")
    
    people = []
    for person in people_1():
        people.append(person)
    sum_age = pipeline.run(people,
                        table_name ="people_info",
                        write_disposition="replace")  
    print(sum_age, "\n")
    print("insert the second")

    people = []
    for person in people_2():
        people.append(person)
    sum_age = pipeline.run(people,
                        table_name ="people_info",
                        primary_key="id",
                        write_disposition="merge")
    print(sum_age)