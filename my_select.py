from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session, engine

def select_01():

    """
--SELECT s.fullname AS student_name, ROUND(AVG(g.grade), 2) AS avg_grade
--FROM students s
--JOIN grades g ON s.id = g.student_id
--GROUP BY s.id
--ORDER BY avg_grade DESC
--LIMIT 5;

    :return:
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()

    return result

def select_02():
    """
SELECT s.id, s.fullname, ROUND (AVG(g.grade), 2) AS average_grade
FROM grades g
JOIN students s ON s.id = g.student_id
WHERE g.subject_id = 1
GROUP BY s.id
ORDER BY average_grade DESC
LIMIT 1;
    """

    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(desc('average_grade')).limit(1).all()

    return result


def select_03():
    """
Знайти середній бал у групах з певного предмета:
SELECT g.id, ROUND(AVG(gr.grade), 2) AS avg_grade
FROM groups g
JOIN students s ON g.id = s.group_id
JOIN grades gr ON s.id = gr.student_id
WHERE gr.subject_id = 1
GROUP BY g.id;

    :return:
    """
    result = session.query(Group.id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).join(Group, Student.group_id == Group.id) \
        .filter(Grade.subject_id == 1).group_by(Group.id).all()
    return result

def select_04():
    """
    SELECT AVG(grade) AS overall_avg_grade
FROM grades;
    :return:
    """
    result = session.query(func.avg(Grade.grade).label('overall_avg_grade')).one()
    return result

def select_05():
    """
    Знайти які курси читає певний викладач

SELECT t.fullname, s.name
FROM teachers t
JOIN subjects s ON t.id = s.teacher_id
WHERE t.id = :teacher_id;
    :return:
    """
    result = session.query(Teacher.fullname, Subject.name).join(Subject).filter(Teacher.id == 1).all()
    return result

def select_06():
    """
    Знайти список студентів у певній групі

SELECT s.fullname AS student_name, g.name
FROM students s
JOIN groups g ON s.group_id = g.id
WHERE g.id = :group_id;

    :return:
    """
    #
    # result = session.query(Student.fullname.label('student_name'), Group.name).join(Group).filter(Group.id == 1) \
    #     .all()
    # return result


def select_07():
    """
    SELECT s.fullname AS student_name, g.grade, sb.name
FROM students s
JOIN grades g ON s.id = g.student_id
JOIN subjects sb ON g.subject_id = sb.id
WHERE s.group_id = :group_id AND sb.id = :subject_id;


    :return:
    """

    result = session.query(Student.fullname.label('student_name'), Grade.grade, Subject.name) \
        .join(Group).join(Grade).join(Subject).filter(Group.id == 1, Subject.id == 2).all()
    return result


def select_08():
    """
SELECT t.fullname, AVG(g.grade) AS avg_grade
FROM teachers t
JOIN subjects s ON t.id = s.teacher_id
JOIN grades g ON s.id = g.subject_id
WHERE t.id = :teacher_id
GROUP BY t.id;
    :return:
    """
    result = session.query(Teacher.fullname, func.avg(Grade.grade).label('avg_grade')).select_from(Teacher) \
        .join(Subject).join(Grade).filter(Teacher.id == 1).group_by(Teacher.id).all()
    return result

def select_09():
    """
--Знайти список курсів, які відвідує студент

SELECT s.fullname AS student_name, sb.name
FROM students s
JOIN grades g ON s.id = g.student_id
JOIN subjects sb ON g.subject_id = sb.id
WHERE s.id = :student_id;


    :return:
    """
    result = session.query(Student.fullname.label('student_name'), Subject.name).select_from(Student) \
        .join(Grade).join(Subject).filter(Student.id == 5).all()
    return result

def select_10():
    """
    SELECT s.fullname AS student_name, t.fullname, sb.name
FROM students s
JOIN grades g ON s.id = g.student_id
JOIN subjects sb ON g.subject_id = sb.id
JOIN teachers t ON sb.teacher_id = t.id
WHERE s.id = :student_id AND t.id = :teacher_id;
    :return:
    """
    result = session.query(Student.fullname.label('student_name'), Teacher.fullname, Subject.name)\
        .select_from(Student).join(Grade).join(Subject).join(Teacher)\
        .filter(Student.id == 5, Teacher.id == 1).all()
    return result


if __name__ == '__main__':
    session.configure(bind=engine)
    session = session()
    # print(select_01())
    #print(select_02())
    #print(select_03())
    #print(select_04())
    #print(select_05())
    #print(select_06())
    #print(select_07())
    #print(select_08())
    #print(select_09())
    print(select_10())
