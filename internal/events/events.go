/*

   for analytics, the events  are being created on the clicks, here , defining click event schema , and the producer interface, where every redirect fires an async click event so  the analytics can be built without even querying the database shards


	 for analytics we will take , shortcode, timestamp, ip, country, referer, and user agent on click we will take this

*/

package events

import "time"

// struct for clickEvent

type ClickEvent struct {
	ShortCode string    `json:"short_code"`
	Timestamp time.Time `json:"ts"`
	IP        string    `json:"ip"`
	Country   string    `json:"country"`
	Referer   string    `json:"referer"`
	UserAgent string    `json:"user_agent"`
}

type Producer interface {
	RecordClick(e ClickEvent)

	Close() error
}
