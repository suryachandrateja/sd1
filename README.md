# Python PR Prep

## Key information
### Column Definitions
**t**: Event Type (Engaged Visit or Open)  
**flyer_id**: Merchant Flyer Identifier  
**account_guid**: Internal user account identifier (form of GUID)  
**sid**: 3rd party user account identifier (used as a backup identifier)  
**date**: date of the event  
**time_iso8601**: event date time in the following sample format: `2021-10-01T08:07:26-04:00`

## Definition of Problem
Goal is to generate/aggregate events to form user sessions.  Each event is datetime stamped, and a group of events
would be considered a session.

### Assumptions
1. After 10min of inactivity the user is session is considered ended.