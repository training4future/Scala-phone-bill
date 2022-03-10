# test "Phone bill calculation"

Scala test for recruitment process.

Final code must be uploaded as a Pull Request to Bitbucket platform.
Acess must be sent to the recruiter.

## Introduction

Your monthly phone bill has just arrived, and it’s unexpectedly large. You decide to verify the amount
by recalculating the bill based on your phone call logs and the phone company’s charges.

The logs are given as a string S consisting of N lines separated by end-of-line characters (ASCII code
10). Each line describes one phone call using the following format: “hh:mm:ss,nnn-nnn-nnn”, where
“hh:mm:ss” denotes the duration of the call (in “hh” hours, “mm” minutes and “ss” seconds) and
“nnn-nnn-nnn” denotes the 9-digit phone number of the recipient (with no leading zeros).

## Rules
 Each call is billed separately. The billing rules are as follows:
 
 * If the call was shorter than 5 minutes, then you pay 3 cents for every started second of the call (e.g. for duration “00:01:07” you pay 67 * 3 = 201 cents).
 * If the call was at least 5 minutes long, then you pay 150 cents for every started minute of the call (e.g. for duration “00:05:00” you pay 5 * 150 = 750 cents and for duration “00:05:01” you pay 6 * 150 = 900 cents).
 * All calls to the phone number that has the longest total duration of calls are free. In the case of a tie, if more than one phone number shares the longest total duration, the promotion is applied only to the phone number whose numerical value is the smallest among these phone numbers.

## Objetive

Write a function solution that, given a string S describing phone call logs, returns the amount of
money you have to pay in cents.
For example, given string S with N = 3 lines:


```bash
"00:01:07,400-234-090
 00:05:01,701-080-080
 00:05:00,400-234-090"
```
The function should return 900 (the total duration for number 400-234-090 is 6 minutes 7 seconds,
and the total duration for number 701-080-080 is 5 minutes 1 second; therefore, the free promotion
applies to the former phone number).
Assume that:

* N is an integer within the range [1..100];
* Every phone number follows the format “nnn-nnn-nnn” strictly; there are no leading zeros;
* The duration of every call follows the format “hh:mm:ss” strictly;
* Each line follows the format “hh:mm:ss,nnn-nnn-nnn” strictly; there are no empty lines and spaces.

## Highlights

Solution has been implemented two diferent ways in PhoneBillCalculation.scala PhoneBillCalculationFromFile.scala Scala objects:

In PhoneBillCalculation.scala you can find required solution using scala, spark and spark sql. calculateBill function receive a S Bill log and calculate the result in cents.

In PhoneBillCalculationFromFile.scala you can find required solution using scala, spark and spark sql. calculateBill function read Bill log FROM FILE and calculate the result in cents. This is useful if you use a distributed storage like HDFS or S3.

In both solutions Distributed Dataframe and paralelized variables are used, code is scalable and prepared to run into a clúster modifying only a line of code.

ValidateBillLog object run basic tests and validations.

## Usage

From scala ide run configurations of scala object needed to execute. Main function will execute specific functions.

From console execute jars files needed to execute. Main function will execute specific functions.

## License
[MIT](https://choosealicense.com/licenses/mit/) 