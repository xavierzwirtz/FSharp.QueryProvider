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