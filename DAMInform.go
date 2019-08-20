// Notification and Dashboard Service for DAM
//

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Tkanos/gonfig" // config management support
	"github.com/lib/pq"        // golang postgres db driver
	"gopkg.in/gomail.v2"       // outbound email support
)

// globals
var db *sql.DB                      // db connection
var sessionConfig = configuration{} // runtime config
var gBuild string

// holds the config, populated from config.json
type configuration struct {
	DBhost            string
	DBusr             string
	DBpw              string
	DBPort            string
	ListenPort        string
	DBName            string
	WorkingFolderPath string
	ChangesetPath     string
	TitleInProgress   string
	TitleEmergency    string
	TitleBlocked      string
	TitleUrgent       string
	TitleNotReady     string
}

// called on run, sets up http listener on port defined in config file.
func main() {

	err := gonfig.GetConf("config.json", &sessionConfig)
	if err != nil {
		panic(err) //TODO:
	}

	http.HandleFunc("/", handler)
	initDb()
	defer db.Close()

	log.Println("Listening... (" + sessionConfig.ListenPort + ")")

	err = http.ListenAndServe(":"+sessionConfig.ListenPort, nil)
	if err != nil {
		fmt.Println("ERROR " + err.Error())
	}
}

// initializes the db [postgres] connection with params held in the config file.
func initDb() {

	var err error

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", sessionConfig.DBhost, sessionConfig.DBPort, sessionConfig.DBusr, sessionConfig.DBpw, sessionConfig.DBName)

	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("DAMInform v" + gBuild + " - Successfully connected!")
}

// standard http handler
// see also getDynamic()
func handler(w http.ResponseWriter, r *http.Request) {

	report := ""

	/* 	if getNotificationQueue(&report) {
	   		fmt.Fprintf(w, report)
	   	}
	*/
	switch r.Method {
	case "GET":
		//http.ServeFile(w,r)

		if strings.Contains(r.URL.Path, "Notifications") {
			if getNotificationQueue(&report) {
				fmt.Fprintf(w, report)
			}
		}
		if strings.Contains(r.URL.Path, "Log") {
			if getLog(&report) {
				fmt.Fprintf(w, report)
			}
		}
		if strings.Contains(r.URL.Path, "Dispatch") {

			if doDispatch() {
				fmt.Fprintf(w, report)
			}
		}
	}

}

func getLog(report *string) bool {

	query := `	SELECT message, messagetime, fromcomponent, focusticket, logtype
	FROM public.log order by messagetime desc	
	`

	tabledef := ""
	tableheader := ""
	tablebody := ""

	log.Println("DAMInform.GetLog() ....")
	tableheader += "<h1>Log report</h1><thead><tr>"
	tableheader += fmt.Sprintf("<th>%s</th>", time.Local)
	tablebody += "<tbody>"
	tableheader += "<th ><div><span>" + "" + "</span></div></th>"

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	for rows.Next() {
		message := ""
		focusticket := ""
		logtype := ""
		fromcomponent := ""

		var messagetime pq.NullTime
		//var id int

		err = rows.Scan(
			&message,
			&messagetime,
			&fromcomponent,
			&focusticket,
			&logtype,
		)

		if err != nil {
			log.Println(err.Error())
			return false
		}

		tablebody += "<tr>"
		//		tablebody += fmt.Sprintf("<th class='row-header'> %d </th>", id)
		tablebody += fmt.Sprintf("<td>%s</td>", message)
		tablebody += fmt.Sprintf("<td>%s</td>", focusticket)

		tablebody += fmt.Sprintf("<td>%s</td>", fromcomponent)
		tablebody += fmt.Sprintf("<td>%s</td>", messagetime.Time.Format("2006-01-02 15:04:05")) //			 		ticket.Targetrepositoryenddate = targetrepositoryenddate.Time.Format("2006-01-02")
		tablebody += fmt.Sprintf("<td>%s</td>", logtype)

		tablebody += "</tr>"
	}

	tableheader += "</tr></thead>"

	tablebody += "</tbody>"

	tabledef = tableheader + tablebody

	overlaptemplate, _ := readlines2("html/reporttemplate.html")

	var line string
	for i := range overlaptemplate {
		line = overlaptemplate[i]
		line = strings.Replace(line, "<cdata>%%TABLE%%</cdata>", tabledef, -1)
		*report += line
	}

	return true

}

func doDispatch() bool {

	// needs a place to store the last sent notifiaction
	// select from queue where id > last sent

	// for each row
	// read values
	// lookup owner -> email
	// send email.
	log.Println("DAMInform.doDispatch() ....")

	lastid := -1
	query := ""
	theid := -1
	result := false
	logMessage("Doing Notification Dispatch", "", "INFO")

	query = `SELECT lastnotification
		FROM public.state;`
	rows, err := db.Query(query)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			fmt.Println("pq error:", err.Code.Name())
			logMessage(fmt.Sprintf("Problems querying state : %s", err.Detail), "", "ERROR")
		}
		return false
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&lastid,
		)
	}

	query = `SELECT n.id, "lead", jiraassignee, message, n.asset, n.created, n.notifymgr, n.jirakey 
			FROM public."change" c
			inner join notificationqueue n on n.jirakey = c.jirakey 
			where n.id > $1
			order by n.id asc`

	rows, err = db.Query(query, lastid)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			fmt.Println("pq error:", err.Code.Name())
			logMessage(fmt.Sprintf("Problems querying notifications[%d] : %s", lastid, err.Detail), "", "ERROR")
		}
		return false
	}
	defer rows.Close()

	for rows.Next() {
		thebody := ""
		thejirakey := ""
		theasset := ""
		notifymgr := false
		thelead := ""
		thejiraassignee := ""
		var whencreated pq.NullTime

		err = rows.Scan(
			&theid,
			&thelead,
			&thejiraassignee,
			&thebody,
			&theasset,
			&whencreated,
			&notifymgr,
			&thejirakey,
		)

		toperson := thelead + "@ahs.ca"

		m := gomail.NewMessage()
		m.SetHeader("From", "noreply@ahs.ca", "DAM")
		m.SetHeader("To", toperson)

		if notifymgr {
			m.SetAddressHeader("Cc", "jon@beeby.ca", "coni") // change this to someone useful
		}

		m.SetHeader("Subject", "DAM: "+thejirakey)
		m.SetBody("text/html", thebody)
		//m.Attach("/home/Alex/lolcat.jpg")

		d := gomail.NewDialer("mail", 25, "", "Hm3xU56U")
		logMessage(fmt.Sprintf("Sending notification about %s to %s : %s", thejirakey, toperson, thebody), thejirakey, "DEBUG")

		// Send the email
		if err := d.DialAndSend(m); err != nil {
			logMessage(fmt.Sprintf("Problems sending mail notification [%d] : %s", theid, err.Error()), thejirakey, "ERROR")
			result = false
			break
		}

	}

	if theid > -1 {
		// save a pointer to the last processed notification, so that we can pick up where we left off next time
		_, err = db.Exec(`UPDATE public.state SET lastnotification = $1;`, theid)
		if err != nil {
			if err, ok := err.(*pq.Error); ok {
				fmt.Println("pq error:", err.Code.Name())
				logMessage(fmt.Sprintf("Problems updating state [%d] : %s", theid, err.Message), "", "ERROR")
				return false
			}
		}

	}

	return result

}
func logMessage(message, ticket, logtype string) error {

	sqlStatement := `
	INSERT INTO public.log
	(message, messagetime, fromcomponent, focusticket, logtype)
	VALUES( $1, $2, $3, $4, $5);`
	_, err := db.Exec(sqlStatement, message, time.Now(), "DAMInform v"+gBuild, ticket, logtype)

	return err
}

// loads ticket metadata from database into struct param
func getNotificationQueue(report *string) bool {

	query := `	SELECT id, message, jirakey, asset, created, notifymgr
	FROM public.notificationqueue order by id desc
	`

	tabledef := ""
	tableheader := ""
	tablebody := ""

	log.Println("DAMInform.getNotificationQueue() ....")
	tableheader += "<h1>notifications report</h1><thead><tr>"
	tableheader += fmt.Sprintf("<th>%s</th>", time.Local)
	tablebody += "<tbody>"
	tableheader += "<th ><div><span>" + "" + "</span></div></th>"

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	for rows.Next() {
		message := ""
		jirakey := ""
		asset := ""
		var notifymgr bool
		var created pq.NullTime
		var id int

		err = rows.Scan(
			&id,
			&message,
			&jirakey,
			&asset,
			&created,
			&notifymgr,
		)

		if err != nil {
			log.Println(err.Error())
			return false
		}

		tablebody += "<tr>"
		tablebody += fmt.Sprintf("<th class='row-header'> %d </th>", id)
		tablebody += fmt.Sprintf("<td>%s</td>", message)
		tablebody += fmt.Sprintf("<td>%s</td>", jirakey)

		tablebody += fmt.Sprintf("<td>%s</td>", asset)
		tablebody += fmt.Sprintf("<td>%s</td>", created.Time.Format("2006-01-02 15:04:05")) //			 		ticket.Targetrepositoryenddate = targetrepositoryenddate.Time.Format("2006-01-02")
		tablebody += fmt.Sprintf("<td>%s</td>", strconv.FormatBool(notifymgr))

		tablebody += "</tr>"
	}

	tableheader += "</tr></thead>"

	tablebody += "</tbody>"

	tabledef = tableheader + tablebody

	overlaptemplate, _ := readlines2("html/reporttemplate.html")

	var line string
	for i := range overlaptemplate {
		line = overlaptemplate[i]
		line = strings.Replace(line, "<cdata>%%TABLE%%</cdata>", tabledef, -1)
		*report += line
	}

	return true

}

// Readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned iff there is an error with the
// buffered reader
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func readlines2(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	r := bufio.NewReader(file)
	s, e := Readln(r)
	for e == nil {

		lines = append(lines, s)
		s, e = Readln(r)
	}

	return lines, nil
}

/* func buildOverlapFocusPage(tickets []structSingleIssue, focusTicket string, theOverlaps overlapMap) (result []string) {

	var aTicket structSingleIssue

	tabledef := ""
	tableheader := ""
	tablebody := ""

	log.Println("worked....")

	tableheader += "<h1>overlap report</h1><thead><tr>"
	tableheader += "<th></th>"
	tablebody += "<tbody>"
	tableheader += "<th ><div><span>" + focusTicket + "</span></div></th>"

	for _, aTicket = range tickets {

		nCount := 0
		key := aTicket.ID + "_" + focusTicket

		nCount = strings.Count(theOverlaps[key], "~")
		if nCount == 0 {
			continue
		}

		tablebody += "<tr>"
		tablebody += "<th class='row-header'>" + aTicket.ID + "</th>"

		overlapAssets := strings.Split(theOverlaps[key], "~")
		overlapDetails := ""

		for _, asset := range overlapAssets {
			if asset == "" {
				continue
			}

			overlapDetails += "" + strings.Title(strings.Replace(string(asset), ".OET", "", -1)) + "<br>"
		}

		if (nCount) > 0 {

			tablebody += fmt.Sprintf("<td>%s</td>", overlapDetails)

		} else {
			tablebody += "<td></td>"
		}

		tablebody += "</tr>"
	}
	tableheader += "</tr></thead>"

	tablebody += "</tbody>"

	tabledef = tableheader + tablebody

	overlaptemplate, _ := readlines2("static/overlapfocus.html")

	var line string
	for i := range overlaptemplate {
		line = overlaptemplate[i]
		line = strings.Replace(line, "<cdata>%%TABLE%%</cdata>", tabledef, -1)
		result = append(result, line)
	}

	return result
} */
