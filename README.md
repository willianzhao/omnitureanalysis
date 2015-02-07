# omnitureanalysis
A set of programs to analyze Omniture clickstream log

## What's included 

### Public lib
 - A framework to parse and get Omniture clickstream log (400 fields)
 - General data model
 - open source geography lookup solution

### Transaction Stitching

This project is to solve the problem of conversion measurement and attribution.Supposed significant increase is found in traffic on mWeb, but the conversion is lower. Do some of these increased visitors on mWeb possibly browsing on mWeb and then making purchases on a different platform (desktop or native app ?) If so, the conversion numbers may not be as bad on mWeb, if we attribute all such visitors's purchases back to where they started (mWeb) their discovery/research. In general, what are the patterns of our users  ? How many of them are browsing on mWeb, and buying for the same event on desktop eventually ? How many of them are buying for events in the same genre on another platform ? How many of them are just doing un-correlated searches on mWeb ?  Understanding the user behavior and the path to how they finally chose to buy the tickets, across devices/platforms would be very telling as to where the problems might be. 

To solve this problem we want to break this in 3 different tables or data sets.
- Identify those transactions for which users  visited the same event on Mobile web/IPad Apps/Mobile Apps before buying the ticket on Desktop.
- Identify those transactions for which users visited  and purchased the tickets for the same genre after visiting the Mobile web/IPad Apps/Mobile Apps. .I.e. User “U1" visited San Francisco Giants events on mobile web but buys Oakland A's tickets from Desktop. Another example is a user "U2" browsed NBA Play off Game 4 on Mobile Suites and purchased tickets for Game 5 on desktop
- Identify those transactions for which users visited and purchased for completely different events after browsing for some events on Mobile web/IPad Apps/Mobile Apps. .i.e. User “U1" visited San Francisco Giants events on mobile web but buys Rock,Pop and hip hop ticket on Desktop

### Orders by Local Time

Using the open source geography lookup solution, convert the UTC order time to its local timezone and analyze the user purchase pattern.

## License
This software is issued under the Apache 2 license.
