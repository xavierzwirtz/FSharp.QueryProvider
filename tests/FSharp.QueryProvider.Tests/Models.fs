module Models

type Department =
    { DepartmentId : int
      DepartmentName : string
      VersionNo : int }

type Employee =
    { EmployeeId : int option
      EmployeeName : string option
      DepartmentId : int option
      VersionNo : int option
      PersonId : int }

type Address =
    { AddressId : int
      Street : string
      VersionNo : byte array }

type JobKind =
| Salesman = 0
| Manager = 1

type Person =
    { PersonId : int
      PersonName : string
      JobKind : JobKind
      VersionNo : int }

type CompKeyEmployee =
    { EmployeeId1 : int
      EmployeeId2 : int
      EmployeeName : string
      VersionNo : int }

type Duplication =
    { aaa : int
      aaa1 : int
      bbb : string }
      
type NoId =
    { Name : string
      VersionNo : int }

type NoVersion =
    { Id : int
      Name : string }

module Data =
    let johnDoe = {
        PersonId = 1
        PersonName = "john doe"
        JobKind = JobKind.Salesman
        VersionNo = 5
    }
    let jamesWilson = {
        PersonId = 2
        PersonName = "james wilson"
        JobKind = JobKind.Manager
        VersionNo = 6
    }
    let bobHoffman = {
        PersonId = 3
        PersonName = "bob hoffman"
        JobKind = JobKind.Salesman
        VersionNo = 7
    }
module DataReaderData =
    let persons : obj list list= [
        [1; "john doe"; 0; 5]
        [2; "james wilson"; 1; 6]
        [3; "bob hoffman"; 0; 7]
    ]