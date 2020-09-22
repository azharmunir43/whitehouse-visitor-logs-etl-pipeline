## White House Logs Data Analysis

### Dataset

White House visitor logs, also known as the White House Worker and Visitor Entry System (WAVE), are the guestbook records of individuals visiting the White House to meet with the President of the United States or other White House officials. Dataset consists of around 6M records. [Source](https://obamawhitehouse.archives.gov/briefing-room/disclosures/visitor-records) 

**Schema**

Columns included in dataset are: -

> `UIN - Appointment Number` 
> `BDG NBR – Badge Number`
> `Access Type - Type of access to the complex (VA = Visitor Access)`
> `TOA – Time of Arrival`
> `POA –  Post of Arrival`
> `TOD – Time of Departure` 
> `POD – Post of Departure`
> `APPT_MADE_DATE – Date the Appointment was made.`
> `APPT_START_DATE – Date and time for which the appointment was scheduled`
> `APPT_END_DATE – Date and time for which the appointment was scheduled to end`
> `APPT_CANCEL_DATE – Date the appointment was canceled, if applicable`
> `Total_People- The total number of people scheduled for a particular appointment per requestor`
> `LAST_UPDATEDBY – Identifier of officer that updated record`
> `POST – Computer used to enter appointment`
> `LastEntryDate – Most recent update to appointment`
> `TERMINAL_SUFFIX - Identifier of officer that entered appointment`
> `visitee_namelast – Last name of the visitee`
> `visitee_namefirst – First name of the visitee`
> `MEETING_LOC – Building in which meeting was scheduled`
> `MEETING_ROOM – Room in which meeting was scheduled`
> `CALLER_NAME_LAST – Last name of the individual that submitted the WAVES request`
> `CALLER_NAME_FIRST – First name of the individual that submitted the WAVES request`
> `CALLER_ROOM – Room from which the appointment was made` 
> `Description – Comments added by the WAVES requestor`

### Environment Setup

Windows / Linux

```shell
pip install -r requirements.txt
```

In addition to that Apache Spark need to be setup locally or in cloud. 

### Usage Example

Once the environment is setup, pipeline can be run via following command

```shell
python run.py
```