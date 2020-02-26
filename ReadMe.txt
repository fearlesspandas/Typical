Typical is a framework that implements a typesafe grammer on Spark DataFrames.
By having the programmer specify column dependencies explicitly, Typical can guarentee
data consistency at compile time. This means all data access inconsistencies will result in
compilation errors, ensuring job success in a type theoretic way.

Important Note: Currently Typical is not a production ready framework and should be used entirely
 at your own discretion.


The syntax works through the following rules:

    1. An initial dataframe must be declared implicitly. This acts as a universal data source across your any Typical types in scope.
    2. Axioms are mapped to columns with the following code:

           class Thing extends AXIOM[Thing]

       this maps the Thing type to the column "Thing" in the initial dataframe declared in step 1.
       Thing is then an AXIOM[Thing] which is an extension of type COL[Thing]
       Important Note: If the column does not exist, you will still see this error at run time. This particular step as
       right now cannot yet be verified at compile time.
    3. COL's are the generic typesafety wrapper around the spark column type. They are abstract and cannot be instantiated directly
    but are easily extended as
        class T[A] extends COL[A<:COL[_]](coldef: org.apache.spark.sql.Column)
    This is not exactly syntactically correct, but a reusing our Thing class we could in fact do something like the following:
        class T extends COL[Thing](new Thing)
    Why would you want to use this? Well COL's also provide typesafe access to data. The next rule will explain what that's used for in
    greater detail, but generally speaking, constructing a COL[Thng] will give you access to the columnar data of Thing, allowing you to
    write normal spark code using any COL[Thing]. What's even better about COL's is there is a convenient

Typical does not yet support joins and it's recommended that data is prejoined