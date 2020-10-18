---
layout: post
title: "A Formal Description of Database Normalization"
author: "Damien Tseng"
categories: "Data-Warehouse"
tags: [Data Warehouse]
mathjax: true
---
<br />
Dimensional modeling is a great technique for building data warehouses. When it is carried out in the context of big data, the denormalization is taken to the extreme. After working with dimensional modeling for some time, I felt an urge to revisit the database theories learned from school, so as to be sure about what rules are being broken here.  

Database normalization describes the way to construct relational databases under the restrictions of normal forms. The fundamental motivation is to formally reduce data redundancy and subsequently improve data consistency.

Normal forms declare what conditions should be met to get a certain level of normalization. These are the forms ordered increasingly by the level of restrictions: $$UNF<1NF<2NF<3NF<BCNF<...$$ A database must abide by all the lower-level forms before meeting higher ones. Higher forms require more restrictions, which also mean heavier I/O. Generally speaking, a database that meets the 3NF is considered sufficiently normalized.
# Functional Dependency
In relational database theories, a functional dependency is a constraint between any two sets of attributes in a relation (i.e. a table). If the set $$B$$ is functionally determined by the set $$A$$, we can formally express it as $$A\to B$$. Or we can say that $$B$$ is a function of $$A$$, i.e., there is a function $$f$$ which  makes $$B=f(A)$$. Take the function $$y=f(x)=x^2$$ for instance, there is exactly one value $$y$$ in the codomain that corresponds to any given $$x$$. Here is a more concrete example, a course enrollment table $$STT$$ with the following schema. Let's assume that each teacher only teaches **one** class, and a student can only choose one teacher/class for each subject. When the student number and name is given along with the subject, we can answer with certainty to the question of which teacher is chosen by a specific student. More formally, we have a functional dependency$$\{student\_id, student\_name, subject\} \to \{teacher\_id\}$$.

| _STT_ |
| --- |
| _student_id_ |
| _student_name_ |
| _subject_ |
| _teacher_id_ |
| _teacher_name_ |

## Partial Functional Dependency
For two sets $$A$$ and $$B$$ that satisfies $$A\to B$$, if there is at least one proper subset $$C\subsetneq A$$, which makes $$C\to B$$, then we say $$A$$ and $$B$$ satisfies a partial functional dependency $$A \xrightarrow{P} B$$. Recalling the course enrollment table $$STT$$, a student is usually identifiable by her student number, so we have a functional dependency$$\{student\_id, subject\} \to \{teacher\_id\}$$. Since$$\{student\_id, subject\}$$ is a proper subset of $$\{stutend\_id, student\_name, subject\}$$, we have a conclusion that$$\{stutend\_id, student\_name, subject\} \xrightarrow{P} \{teacher\_id\}$$.
## Full Functional Dependency
For two sets $$A$$ and $$B$$ that satisfies $$A\to B$$, if there is no such proper subset $$C\subsetneq A$$, which makes $$C\to B$$, then we say $$A$$ and $$B$$ satisfies a full functional dependency $$A \xrightarrow{F} B$$. A concrete example is $$\{student\_id, subject\} \xrightarrow{F} \{teacher\_id\}$$.
## Properties of Functional Dependency
Assuming $$U$$ is a universal set, and $$A,B,C,D,X,Y$$ are some subsets of $$U$$, then we have the following axiomatizations.

1. **Reflectivity**. If $$B \subset A$$, then $$A \to B$$. This is also known as trivial functional dependency.
1. **Augmentation**. If $$A \to B$$, then $$AC \to BC$$.
1. **Transitivity**. If $$A \to B$$, and $$B \to C$$, then $$A \to C$$. We may also say that $$C$$ has a transitive functional dependency on $$A$$.  



Some other properties that can be deduced from the above three.  

4. **Union**. If $$A \to B$$, and $$A \to C$$, then $$A \to BC$$.  
Proof:  
&nbsp; &nbsp; ∵$$A \to B$$, ∴ by Augmentation we have $$AA \to AB$$，∴$$A \to AB$$;  
&nbsp; &nbsp; ∵$$A \to C$$, ∴ by Augmentation we have $$AB \to BC$$;  
&nbsp; &nbsp; finally, by Transitivity, we have $$A \to BC$$.  

5. **Decomposition**. If $$A \to BC$$, then $$A \to B$$ and $$A \to C$$.  
Proof:  
&nbsp; &nbsp; ∵$$B \subset BC$$, ∴$$BC \to B$$;  
&nbsp; &nbsp; ∵$$A \to BC$$, by Transitivity we know $$A \to B$$;  
&nbsp; &nbsp; similar for $$A \to C$$.  

6. **Composition**. If $$A \to B$$, and $$X \to Y$$, then $$AX \to BY$$.  
Proof:  
&nbsp; &nbsp; ∵ $$A \to B$$, ∴ $$AX \to BX$$;  
&nbsp; &nbsp; ∵ $$X \to Y$$, ∴ $$BX \to BY$$;  
&nbsp; &nbsp; by Transitivity, we have $$AX \to BY$$.  

7. **Pseudo Transitivity**. If $$A \to B$$ and $$BC \to D$$, then $$AC \to D$$.  
Proof:  
&nbsp; &nbsp; ∵ $$A \to B$$, ∴ $$AC \to BC$$;  
&nbsp; &nbsp; ∵ $$BC \to D$$, by Transitivity, $$AC \to D$$.  

# 1NF
First normal form requires that all the attributes in a relation should be atomic. Non-atomic attributes are those with multiple fields so that they can be further split into more fine-grained attributes. There have been disputes about atomicity, mainly caused by its ambiguous definition. The way to resolve the ambiguity is to restrict the description context to a certain domain. Take the ID card number for citizens in China for instance. It seems that an ID card number is atomic. While in fact the number can be further sliced into an address code, a birthday code, a sequence code, and a check code.
# 2NF
Second normal form. Apart from meeting 1NF, 2NF requires that all the non-prime attributes in a relation should be functionally dependent on any proper subset of any candidate key. That is, non-prime attributes can’t be **partially** functionally dependent on the candidate key. The set $$\{student\_id,subject\}$$ is the only candidate key in the relation $$STT$$, since $$\{student\_id\} \xrightarrow{F} \{student\_name\}$$, then$$\{student\_id,subject\} \xrightarrow{P} \{student\_name\}$$. Therefore, $$STT$$ doesn’t conform to 2NF. The way to normalize is to remove _student_name_ from $$STT$$, maybe to another table, $$STUDENT$$ with the _student_id_ as the primary key and the _student_name_ as a non-prime attribute.
# 3NF
Third normal form. There are two ways to interpret it.

1. Based on 2NF, 3NF requires that non-prime attributes shouldn’t have any **transitive** functional dependency on the candidate key.
1. It enforces that any functional dependency in the relation should have one of these properties:
   1. It is a trivial functional dependency, to wit, $$B \subset A$$.
   1. $$A$$ is a super-key of the relation.
   1. Let $$C=B-A$$, then all the attributes in $$C$$ are prime attributes.

Because $$\{student\_id,subject\} \to \{teacher\_id\}$$ and $$\{teacher\_id\} \to \{teacher\_name\}$$, so _teacher\_name_ is transitively functionally dependent on the candidate key. Thus $$STT$$ doesn’t conform to 3NF. Or put it another way, $$\{teacher\_id\} \to \{teacher\_name\}$$ is not a trivial functional dependency, $$\{teacher\_id\}$$ is not a super-key, and _teacher\_name_ is not a prime attribute. The way of normalization is to decomposed the dependency _teacher\_id_ and _teacher\_name_ into a $$TEACHER$$ relation.
# BCNF
Boyce–Codd normal form. Based on the second way of interpretation of 3NF, it removes the third property (2.3), thus leading to a more restricted form than 3NF. That is to say, BCNF requires that all the functional dependencies $$A \to B$$ should satisfy one of these two properties:

   1. It is a trivial functional dependency, $$B \subset A$$.
   1. $$A$$ is a super-key of the relation.

Let’s assume the table is already normalized so that it conforms to 3NF. So now it contains these attributes: $$\{student\_id,subject, teacher\_id\}$$. The dependency$$\{student\_id,subject\}\to \{teacher\_id\}$$holds. Recall the assumption that each teacher only teaches one class, so we have$$\{teacher\_id\} \to \{subject\}$$. Since $$\{teacher\_id\}$$ is not a super-key, the relation doesn’t conform to BCNF.  

Not all non-BCNF tables are capable of being split into tables that satisfy BCNF. The above case is one that is impossible to achieve BCNF. It has a pattern of dependencies like$$\{AB \to C, C \to B\}$$.

# Denormalization
Data warehouse and OLAP systems are cut out for massive data storage, processing, and analysis. They function in ways fundamentally different from those of OLTP systems.  

OLTP systems are mostly about individual transactions, where only a handful of tables and records are involved. Contrary to that, typical queries in OLAP systems tend to do aggregations against data in bulk so as to generate insights for better business strategies. Therefore, reading efficiency is the key performance indicator of OLAP systems. And since data are usually loaded through offline batch (idempotent) jobs, it's much easier to retain data consistency in OLAP systems. Consequently, data warehouses tend to adopt dimensional modeling, and tables denormalizing is quite ubiquitous.
