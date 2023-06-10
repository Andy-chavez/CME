create table if not exists amchavezaltamirano_coderhouse.coronal_mass_ejection(
datetime_event text not null,
latitude float not null,
longitude float not null,
halfAngle float not null,
speed float not null,
type_event text not null,
isMostAccurate boolean not null,
associatedCMEID varchar(28),
note varchar(600),
catalog_event text,
link text,
date_event date not null,
time_event time not null,
id int identity(1,1),
primary key(id))
distkey(date_event)
compound sortkey(id, date_event,time_event);