a = load '$input' as (f1:chararray,f2:int);
b = group a by f1;
c = foreach b generate group,SUM(a.f2);
