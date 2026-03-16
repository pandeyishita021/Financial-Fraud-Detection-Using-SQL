create database fraud_dataset;
use fraud_dataset;

create table transactions(
step int,
type varchar(20),
amount decimal(15,2),
nameOrig varchar(20),
oldbalanceOrig decimal(15,2),
newbalanceOrig decimal(15,2),
nameDest varchar(20),
oldbalanceDest decimal(15,2),
newbalanceDest decimal(15,2),
isFraud tinyint,
isFlaggedFraud tinyint
);

select*from transactions;
select count(*) from `fraud_dataset`.`ps_20174392719_1491204439457_log`;
insert into transactions
select*
from `fraud_dataset`.`ps_20174392719_1491204439457_log`;
select count(*) from transactions;
select*from transactions limit 10;

-- 1. Detecting Recursive Fraudulent Transactions
with recursive fraud_chain as(
select nameOrig as initial_account,
nameDest as next_account,
step,
amount
from 
transactions
where isFraud=1 and type='TRANSFER' 

union all

select fc.initial_account,
t.nameDest,t.step,t.amount
from fraud_chain fc
join transactions t
on fc.next_account=t.nameOrig and fc.step<t.step
where t.isFraud=1 and t.type='TRANSFER')

select*from fraud_chain;


-- 2. Analyzing Fraudulent Activity Over Time
with rolling_fraud as (select nameOrig, step,
sum(isFraud) over (partition by nameOrig order by step rows between 4 preceding and current row) as fraud_rolling
from transactions)
select * from rolling_fraud;


-- 3. Complex Fraud Detection Using Multiple CTEs
with large_transfer as(
select nameOrig, step, amount from transactions where type='TRANSFER' and amount>500000),
no_balance_change as(
select nameOrig, step, oldbalanceOrig, newbalanceOrig from transactions where oldbalanceOrig=newbalanceOrig),
flagged_transaction as(
select nameOrig, step from transactions where isFlaggedFraud=1)

select nameOrig from large_transfer
union
select nameOrig from no_balance_change
union
select nameOrig from flagged_transaction;


-- 4. Balance Consistency Check
with CTE as(
select amount, nameOrig, oldbalanceDest, newbalanceDest, (amount+oldbalanceDest) as new_updated_balance
from transactions
)
select * from CTE where new_updated_balance=newbalanceDest;


-- 5. Detect Transactions With Zero Balance Before Or After  
select nameOrig, oldbalanceOrig, newbalanceOrig from transactions where oldbalanceOrig=0 or newbalanceOrig=0;

