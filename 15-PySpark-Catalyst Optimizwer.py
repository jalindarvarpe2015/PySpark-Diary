# Catalyst Optimizer
'''
scala program provided by spark for dataframe/sql API, that automatically finds out the most efficient plan to execute daya operations specified in the user code

'''
'''
The catalyst optimizer will optimize the execution plan for structured API's (Data Frames and Datasets), and the catalyst optimizer is also responsible for converting DSL or SQL expressions into low-level RDD operations.

Catalyst Optimizer is a rule-based optimization engine, and we can add our own customized rules.

1) Unresolved logical plan (parsed logical plan):
The catalyst optimizer first creates a parsed logical plan from DSL/SQL expressions. Then it checks the syntax of our query. If anything is wrong, it will throw a parsing error. If the syntax is correct, it will create the analyzed logical plan.

2) Logical Plan:
In this step, the table names and column names are checked with help from the catalog. If the column/table names are not incorrect, an analysis exception will be thrown. If the syntax is correct, it will go to the next step, i.e., the optimized logical plan.

3) Optimized logical plan:
The actual optimization is done in this step. This step will have different rules, like pushing the filter, combining the filters, etc., In this step, rearrangement of the query will also be done.
for example, if there is any filter in the query having join, then it will move the filter before join operation to reduce the size of data involved for join operation. A physical plan is then calculated from the optimized plan.

4) Physical plan
In this step, based on operations in the query expression, one or more physical plans are created. The physical plan is nothing but an actual execution plan. The physical plans will go to the cost-based optimizer.

5) Selecting a physical plan:
Suppose we have two physical plans for the query that is having join. The plan that involves performance improvement will be selected. (Selecting the broadcast join over shuffle join if one of the data frame is small)

After selecting a physical plan, our DSL/SQL expressions will be converted into low-level RDD operations.

'''

'''
# catalyst optimizer is only applicablle for structure api like dataframe and spark-sql for converting into low lvel rdd operation


#why it is not applicable for rdd?

what is rdd ?
rdd isa distributted collection of data element spread across many machines in the cluster .

what is dataframe?
a dataframe is a distributed collection of data orgnized into named columns. it is conceptually equal to a table in a relational databases.


-both are same in immutable,in memory , resilent, distributed computing

-dataframe differ from rdd in maintaining the structure of the underlying data in the form of schema. thus dataframe is equivalent to table in fdatabaese

'''

#RBO - Ruled based optimizer
#CBO - Cost Based Optimizer

#RBO --> set of predefined rules is used to design the logical plan
#CBO ---> based on available statictics collection of underlying data, cost is estimated and best efficient execution approach is decided based on cost

