import pandas as pd

def students_and_examinations(students: pd.DataFrame, subjects: pd.DataFrame, examinations: pd.DataFrame) -> pd.DataFrame:
    df = students.merge(examinations, on = 'student_id',how= 'right')
    df = df.sort_values(by = 'student_id')
    df = df.groupby(['student_id','student_name','subject_name']).size().reset_index(name ='attended_exams')
    return df