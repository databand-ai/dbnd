
This package should contain code that can run without databand


 
If we put code in this package that imports dbnd* packages, we'll have a problem with circle dependencies:
    * import databand
        * import this package
            * from airflow code : import databand.some_package     (we are still in the context of import databand!!!)
     
