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

	//"io/ioutil"
	"path/filepath"

	"github.com/Tkanos/gonfig" // config management support
	"github.com/lib/pq"        // golang postgres db driver
	"gopkg.in/gomail.v2"       // outbound email support
)

// globals5
var db *sql.DB                      // db connection
var sessionConfig = configuration{} // runtime config
var gBuild string

const cRELEASEDVERSIONSUFFIX = " [Released asset]"

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
	SubjectPrefix     string
	ManagersEmail     string
}

// called on run, sets up http listener on port defined in config file.
func main() {

	if len(os.Args) > 1 {
		aSwitch := os.Args[1]
		if strings.ToLower(aSwitch) == "-v" {
			println("DAMInform v" + gBuild)
			return
		}
	}

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

	switch r.Method {
	case "GET":
		if strings.Contains(r.URL.Path, "/html/") {
			fs := http.FileServer(http.Dir("./"))
			fs.ServeHTTP(w, r)
		}

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

		if strings.Contains(r.URL.Path, "WhereUsed") {
			if getWUR(&report, r.URL.Path) {
				fmt.Fprintf(w, report)
			}
		}

		if strings.Contains(r.URL.Path, "Dispatch") {

			if doDispatch() {
				fmt.Fprintf(w, report)
			}
		}

		if strings.Contains(r.URL.Path, "FixTicket") {

			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}

			ticketToFix := strings.Trim(params[1], "/") // operation type

			if fixTicket(ticketToFix) {
				w.WriteHeader(http.StatusOK)

			} else {
				w.WriteHeader(http.StatusTeapot)

			}
		}

		if strings.Contains(r.URL.Path, "IntegrityCheck") {

			assetProblem := make(map[string]string)

			if doIntegrityCheck(assetProblem) {
				if len(assetProblem) > 0 {
					w.WriteHeader(http.StatusTeapot)
				} else {
					w.WriteHeader(http.StatusOK)
				}
			}
		}
/* 
		if strings.Contains(strings.ToLower(r.URL.Path), strings.ToLower("StartRefresh")) {
			if refreshAssetStart(r.RequestURI) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("☄ HTTP status code returned!"))
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("☄ HTTP status code returned!"))
			}
		}

		if strings.Contains(strings.ToLower(r.URL.Path), strings.ToLower("EndRefresh")) {
			if refreshAssetEnd(r.RequestURI) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("☄ HTTP status code returned!"))
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("☄ HTTP status code returned!"))
			}
		} */

	}

}

func testDirectoryMonitoring(path string) bool {

	// create a test file in each folder then query activity table for corresponding records

	return true
}

func fixTicket(ticket string) bool {

	logMessage("INTEGRITY: fixTicket() called", ticket, "INFO")

	subDirToSkip := "downloads"
	basePath := sessionConfig.ChangesetPath
	damassetmap := make(map[string]string)

	query := `SELECT folder, filename, fullfilepath
			FROM public.damasset`

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	for rows.Next() {
		folder := ""
		filename := ""
		fullfilepath := ""

		err = rows.Scan(
			&folder,
			&filename,
			&fullfilepath,
		)

		damassetmap[folder+"~"+filename] = fullfilepath
	}

	err = filepath.Walk(basePath+"/"+ticket, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if info.IsDir() && info.Name() == subDirToSkip {
			//			fmt.Printf("skipping a dir without errors: %+v \n", info.Name())
			return filepath.SkipDir
		}

		if strings.HasSuffix(path, ".oet") {
			relpath, err := filepath.Rel(basePath, path)
			if err != nil {
				return err
			}

			results := strings.Split(relpath, "/")
			if len(results) > 0 {
				ticket := results[0]
				asset := filepath.Base(relpath)
				key := ticket + "~" + asset

				// either the file is in damasset or it isn't
				if _, ok := damassetmap[key]; ok {
					// asset exists in damassets and in filesystem
					// so asset can be removed from map.
					delete(damassetmap, key)
				} else {
					logMessage("INTEGRITY: Attempting to fix - Missing in damasset: "+asset, ticket, "ERROR")

					sqlStatement := `INSERT INTO public.activity_local
					(filename, operation, directory, "time", optime)
					VALUES($1, $2, $3, 0, $4);`

					_, err = db.Exec(sqlStatement,
						asset,
						"MODIFY",
						filepath.Dir(path),
						time.Now(),
					)

					if err, ok := err.(*pq.Error); ok {

						logMessage("pq error:"+err.Code.Name()+" - "+err.Message, "", "ERROR")
					}

				}

			}
		}

		return nil
	})
	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", basePath, err)
		return false
	}

	return true
}

func doIntegrityCheck(AssetProblem map[string]string) bool {

	// for each ticket

	subDirToSkip := "downloads"
	basePath := sessionConfig.ChangesetPath

	damassetmap := make(map[string]string)

	query := `SELECT folder, filename, fullfilepath
			FROM public.damasset`

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	for rows.Next() {
		folder := ""
		filename := ""
		fullfilepath := ""

		err = rows.Scan(
			&folder,
			&filename,
			&fullfilepath,
		)

		damassetmap[folder+"~"+filename] = fullfilepath
	}

	err = filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if info.IsDir() && info.Name() == subDirToSkip {
			return filepath.SkipDir
		}

		if strings.HasSuffix(path, ".oet") {
			relpath, err := filepath.Rel(basePath, path)
			if err != nil {
				return err
			}

			results := strings.Split(relpath, "/")
			if len(results) > 0 {
				ticket := results[0]
				asset := filepath.Base(relpath)
				key := ticket + "~" + asset

				// either the file is in damasset or it isn't
				if _, ok := damassetmap[key]; ok {
					// asset exists in damassets and in filesystem
					// so asset can be removed from map.
					delete(damassetmap, key)
				} else {
					AssetProblem[key] = "missing in damasset"
					fmt.Printf(sessionConfig.SubjectPrefix+"ERROR - INTEGRITY %q: Missing in damasset - ticket: %q template :%q \n", sessionConfig.ChangesetPath, ticket, asset)
					logMessage("INTEGRITY: "+basePath+" -  Missing in damasset: "+asset, ticket, "ERROR")
				}

			}
		}

		return nil
	})
	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", basePath, err)
		return false
	}

	for a := range damassetmap {
		AssetProblem[a] = "in damasset, not in filesystem"
		fmt.Printf(sessionConfig.SubjectPrefix+"ERROR - INTEGRITY %q: Missing in filesystem (in damasset) - %q \n", sessionConfig.ChangesetPath, a)
		logMessage("INTEGRITY: Missing in filesystem (in damasset) : "+sessionConfig.ChangesetPath, a, "ERROR")

	}

	if len(AssetProblem) > 0 {
		// need to queue a notification now....
		sqlStatement := `
		INSERT INTO public.notificationqueue
		(message, jirakey, asset, created, notifymgr, lead)
		VALUES( $1, $2, $3, $4, $5, $6);`

		notificationmessage := "Problems with AssetTracking on " + sessionConfig.ChangesetPath + ". DAMLogger should be restarted for this environment."

		_, err = db.Exec(sqlStatement,
			notificationmessage,
			"",
			"",
			time.Now(),
			true,
			"jon.beeby",
		)

		if err, ok := err.(*pq.Error); ok {
			logMessage("pq error:"+err.Code.Name()+" - "+err.Message, "", "ERROR")
		}
	}

	return true
}
/* 
func refreshAssetStart(assetdef string) bool {

	result := true

	fmt.Println("BEGIN RefreshAssetStart.  ")

	assetdef = strings.Replace(strings.ToUpper(assetdef), "/STARTREFRESH,", "", 1)

	splits := strings.Split(assetdef, "~")

	theTemplateName := splits[0]
	theFolder := splits[1]
	theID := splits[2]

	logerr := logMessage("Locking out "+theTemplateName+" for refresh.", theFolder, "INFO")
	if logerr != nil {
		fmt.Println("DAMInform v" + gBuild + " ERRRO when trying to log : " + logerr.Error())
		result = false
	}

	sqlStatement := `
		INSERT INTO public.locktable
		(folder, tablename, ident)
		VALUES($1, $2, $3);`

	_, err := db.Exec(sqlStatement, theFolder, "damasset", theID)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			fmt.Println("pq error:", err.Code.Name())
			logMessage("Problems Locking out "+theTemplateName+" for refresh."+err.Detail, theFolder, "ERROR")
			result = false
		}
	}
	fmt.Println("END RefreshAssetStart.  ")

	return result
}
func refreshAssetEnd(assetdef string) bool {
	result := true
	fmt.Println("BEGIN refreshAssetEnd.  ")
	time.Sleep(5 * time.Second)

	assetdef = strings.Replace(strings.ToUpper(assetdef), "/ENDREFRESH,", "", 1)
	splits := strings.Split(assetdef, "~")

	theTemplateName := splits[0]
	theFolder := splits[1]
	theID := splits[2]

	logerr := logMessage("Refresh of "+theTemplateName+" completed, resetting modified and stale state.", theFolder, "INFO")
	if logerr != nil {
		fmt.Println("DAMInform v" + gBuild + " ERRRO when trying to log : " + logerr.Error())
		result = false
	}

	sqlStatement := `update damasset set modified = false, islatest = true where UPPER(resourcemainid) = UPPER($1) and UPPER(folder) = UPPER($2)`
	_, err := db.Exec(sqlStatement, theID, theFolder)

	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			result = false
			fmt.Println("pq error:", err.Code.Name())
			logMessage("Problems updating damasset state after refresh."+err.Detail, theFolder, "ERROR")
		}
	}

	logerr = logMessage("Unlocking  "+theTemplateName+" after refresh.", theFolder, "INFO")
	if logerr != nil {
		fmt.Println("DAMInform v" + gBuild + " ERRRO when trying to log : " + logerr.Error())
		result = false
	}

	sqlStatement = `
		DELETE from public.locktable
		WHERE UPPER(folder) = UPPER($1) and
			UPPER(tablename) = UPPER($2) and 
			UPPER(ident) = UPPER($3);`

	_, err = db.Exec(sqlStatement, theFolder, "damasset", theID)

	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			result = false
			fmt.Println("pq error:", err.Code.Name())
			logMessage("Problems unlocking "+theTemplateName+" after refresh."+err.Detail, theFolder, "ERROR")
		}
	}

	fmt.Println("END RefreshAssetEnd.  ")

	return result

} */

func getParents(assetID string) map[string]string {

	var m map[string]string

	m = make(map[string]string)

	query := ` select ms_p.name, templateid, c.cid, rels.isReleased
	 from public.mirrorstate_relationships rels
	 left join ckmresource c on rels.parentid = c.resourcemainid
	 inner join public.mirrorstate ms_p on ms_p.templateid = rels.parentid
			and childid = $1 order by 1 desc`

	rows, err := db.Query(query, assetID)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		parentname := ""
		parentid := ""
		parentcid := ""
		isReleasedRelationship := false
		err = rows.Scan(
			&parentname,
			&parentid,
			&parentcid,
			&isReleasedRelationship,
		)

		if isReleasedRelationship {
			parentname += cRELEASEDVERSIONSUFFIX
		}

		m[parentname] = parentid + "~" + parentcid

	}

	return m

}

func addParentRow(parentcid, parenttitle string) string {


	return fmt.Sprintf(`        <tr>
	<td  style=" text-align: center;">   
	<select>
		<option value="dontknow">-</option>
		<option value="yes">Yes</option>
		<option value="no">No</option>
	  </select>
	</td>
	<td data-hyperlink="https://ahsckm.ca/#showTemplate_%s" ><p>• <a  target="_blank" href="https://ahsckm.ca/#showTemplate_%s">%s</a></p></td>
	<td    style=" text-align: center;">   
		<select>
			<option value="dontknow">-</option>
			<option value="yes">Yes</option>
			<option value="no">No</option>
		</select>
	</td>
	<td  style=" text-align: center;">   
	<select>
		<option value="dontknow">-</option>
		<option value="yes">Yes</option>
		<option value="no">No</option>
	</select>
	</td>
	<td  ></td>
	</tr>    `, parentcid, parentcid, parenttitle)

}

func addParentRowNone() string {
	return `
	<tr>
		<td    style=" text-align: center;">   
			<select>
				<option value="dontknow">-</option>
				<option value="yes">Yes</option>
				<option value="no">No</option>
			</select>
		</td>
		<td   ><p>[ none ]</p></td>
		<td   ></td>
		<td   ></td>		
		<td   ></td>		
	</tr>    `

}

func addSection(name string, columnnumber int) string {
	tablebody := ""

	tablebody += fmt.Sprintf("<td data-fill-color='eaecec' data-height='25' data-f-bold='true'  data-a-h='center' style='background-color: rgba(234, 236, 236, 0.6);font-family: Lato; text-align: center;'><b>%s</b></td>", name)

	for i := 0; i < columnnumber; i++ {
		tablebody += "<td data-fill-color='eaecec' style='background-color: rgba(234, 236, 236, 0.6);;'><b></b></td>"
	}

	return tablebody
}

func getWUR(report *string, Path string) bool {

	fmt.Println("DAMInform : getWUR() " + Path)

	parts := strings.Split(Path, ",")

	assetID := parts[1]

	fmt.Println("DAMInform : getWUR() " + assetID)

	
	rows, err := db.Query("select resourcemaindisplayname from ckmresource c where resourcemainid = $1", assetID)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	assetdisplayname := ""

	for rows.Next() {

		err = rows.Scan(
			&assetdisplayname,
		)

		if err != nil {
			log.Println(err.Error())
			return false
		}		
	}

	query :=`select distinct ms_p.filename, ms_p.templateid, ms_c.filename as childname, rels.isReleased, ms_p.cid
		from public.mirrorstate ms_p, public.mirrorstate_relationships rels, public.mirrorstate ms_c
		where rels.parentid = ms_p.templateid
		and childid = ms_c.templateid  
		and childid = $1 order by 1 asc`

	tabledef := ""
	tableheader := ""
	tablebody := ""
	columnnumber := 5

	log.Println("DAMInform.getWUR() ....")

	rows, err = db.Query(query, assetID)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()

	orderpanels := []string{}
	ordersets := []string{}
	smartgroups := []string{}
	others := []string{}
	childname := ""
	isReleasedRelationship := false

	for rows.Next() {
		parentname := ""
		parentid := ""
		parentcid := []byte("") // declared this way to handle null values in Scan() 

		//var parentcidnullable sql.NullString 

		err = rows.Scan(
			&parentname,
			&parentid,
			&childname,
			&isReleasedRelationship,
			&parentcid,
		)

		if err != nil {
			log.Println(err.Error())
			return false
		}

		if isReleasedRelationship {
			parentname += cRELEASEDVERSIONSUFFIX
		}
	
/* 		if parentcidnullable.Valid {
			parentcid, _ = parentcidnullable.Value()
		}

 */		if strings.Contains(strings.ToLower(parentname), "order panel") {
			// add to the panel list
			orderpanels = append(orderpanels, parentname+"~"+parentid+"~"+string(parentcid))
		} else {
			if strings.Contains(strings.ToLower(parentname), "smart group") {
				// add to the panel list
				smartgroups = append(smartgroups, parentname+"~"+parentid+"~"+string(parentcid))
			} else {
				if strings.Contains(strings.ToLower(parentname), "order set") {
					// add to the panel list
					ordersets = append(ordersets, parentname+"~"+parentid+"~"+string(parentcid))
				} else {
					// randoms
					others = append(others, parentname+"~"+parentid+"~"+string(parentcid))
				}
			}

		}

	}
	theTime := fmt.Sprintf("%s", time.Now().Format("Mon Jan _2 2006 @ 15:04"))
	tableheader += "<thead>" +
				"<tr>" +
				"<td style='font-size: x-large; border-top-color: white; border-left: white; border-right: white;' data-f-sz='22'><img width='64' height='64' src='html/AHS-logo.jpg'>" + assetdisplayname + "</b></td>" + 
				"<td style='border-top-color: white; border-left: white; border-right: white;' data-f-sz='22'>" + "</td>" + 								
				"<td style='vertical-align: bottom; border-top-color: white; border-left: white; border-right: white;'>Where Used Report - " + theTime + "</td>" +
				"<td style='border-top-color: white; border-left: white; border-right: white;' data-f-sz='22'>" + "</td>" + 
				"<td style='border-top-color: white; border-left: white; border-right: white;' data-f-sz='22'>" + "</td>" + 
				"<td style='vertical-align: bottom; border-top-color: white; border-left: white; border-right: white;' data-a-wrap='true'>Clinical Knowledge<br>& Content Management</td>" + 				
				"</tr>"

	tableheader += fmt.Sprintf( `<tr style='background: aliceblue;'>
						<td data-fill-color="D8E8F0" data-f-bold='true'><b>Assets containing %s </b></td>
						<td data-fill-color="D8E8F0" data-a-h="center" data-a-h="center" data-a-wrap="true"data-f-bold='true'><b>To be Updated?</b></td>
						<td data-fill-color="D8E8F0" data-a-h="center"  data-a-wrap="true" data-f-bold='true'><b>Assets where the listed Panel or Smart Group is Embedded</b></td>
						<td data-fill-color="D8E8F0"  data-a-h="center" data-a-wrap="true" data-f-bold='true'><b>To be Updated?</b></td>
						<td data-fill-color="D8E8F0"  data-a-h="center" data-a-wrap="true" data-f-bold='true'><b>Task Complete?</b></td>
						<td data-fill-color="D8E8F0" data-a-h="center"  data-f-bold='true'><b>Comments</b></td>
					</td></tr>`, assetdisplayname )

	tablebody += "<tbody>"
	// ------------------------------------------ ORDER PANELS --------------------------------------------------------
	tablebody += "<tr data-height='20' >"
	tablebody += addSection("List of all Order Panels", columnnumber)
	tablebody += "</tr>"

	if len(orderpanels) > 0 {
		for i := range orderpanels {
			bits := strings.Split(orderpanels[i], "~")
			name := bits[0]
			id := bits[1]
			childcid := bits[2]

			parents := getParents(id)
			rowspan := len(parents)

			tablebody += "<tr>"
			if len(parents) > 0 {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a target='_blank' href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", rowspan+1, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))

				for parent := range parents {
					parentbits := strings.Split(parents[parent], "~")
					parentcid := parentbits[1]
					parenttitle := strings.ReplaceAll(parent, ".oet", "")

					tablebody += addParentRow(parentcid, parenttitle)
				}
			} else {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a  target='_blank' href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", 2, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))
				tablebody += addParentRowNone()
			}

			tablebody += "</tr>"
		}
	} else {
		tablebody += "<tr>"
		tablebody += fmt.Sprintf("<td  >%s</td>", "[ none ]")
		tablebody += "</tr>"

	}

	// ------------------------------------------ SMART GROUPS --------------------------------------------------------
	tablebody += "<tr>"
	tablebody += addSection("List of all Smart Groups", columnnumber)
	tablebody += "</tr>"

	if len(smartgroups) > 0 {

		for i := range smartgroups {
			bits := strings.Split(smartgroups[i], "~")
			name := bits[0]
			id := bits[1]
			childcid := bits[2]			
			parents := getParents(id)

			rowspan := len(parents)

			tablebody += "<tr>"
			if len(parents) > 0 {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a target='_blank'  href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", rowspan+1, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))

				for parent := range parents {
					parentbits := strings.Split(parents[parent], "~")
					parentcid := parentbits[1]
					parenttitle := strings.ReplaceAll(parent, ".oet", "")

					tablebody += addParentRow(parentcid, parenttitle)
				}
			} else {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a target='_blank'  href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", 2, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))
				tablebody += addParentRowNone()
			}

			tablebody += "</tr>"
		}
	} else {
		tablebody += "<tr>"
		tablebody += fmt.Sprintf("<td   >%s</td>", "[ none ]")
		tablebody += "</tr>"
	}
	// ------------------------------------------ ORDER SETS --------------------------------------------------------
	tablebody += "<tr>"
	tablebody += addSection("List of all Order Sets", columnnumber)
	tablebody += "</tr>"

	if len(ordersets) > 0 {
		for i := range ordersets {
			bits := strings.Split(ordersets[i], "~")
			name := bits[0]
			id := bits[1]
			parents := getParents(id)
			childcid := bits[2]						
			rowspan := len(parents)

			tablebody += "<tr>"

			if len(parents) > 0 {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a target='_blank'  href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", rowspan+1, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))

				for parent := range parents {
					parentbits := strings.Split(parents[parent], "~")
					parentcid := parentbits[1]
					parenttitle := strings.ReplaceAll(parent, ".oet", "")

					tablebody += addParentRow(parentcid, parenttitle)
				}
			} else {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a  target='_blank' href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", 2, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))
				tablebody += addParentRowNone()
			}



			tablebody += "</tr>"
		}
	} else {
		tablebody += "<tr>"
		tablebody += fmt.Sprintf("<td>%s</td>", "[ none ]")
		tablebody += "</tr>"

	}
	// ------------------------------------------ ALL OTHERS  --------------------------------------------------------
	tablebody += "<tr>"
	tablebody += addSection("List of all others", columnnumber)
	tablebody += "</tr>"

	if len(others) > 0 {
		for i := range others {
			bits := strings.Split(others[i], "~")
			name := bits[0]
			id := bits[1]
			childcid := bits[2]									
			parents := getParents(id)
			rowspan := len(parents)
			tablebody += "<tr>"

			if len(parents) > 0 {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a  target='_blank' href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", rowspan+1, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))

				for parent := range parents {
					parentbits := strings.Split(parents[parent], "~")
					parentcid := parentbits[1]
					parenttitle := strings.ReplaceAll(parent, ".oet", "")

					tablebody += addParentRow(parentcid, parenttitle)
				}
			} else {
				tablebody += fmt.Sprintf("<td   style='font-family:Lato;' rowspan='%d' data-hyperlink='https://ahsckm.ca/#showTemplate_%s' ><p>• <a  target='_blank' href='https://ahsckm.ca/#showTemplate_%s'>%s</a></p></td>", 2, childcid, childcid, strings.ReplaceAll(name, ".oet", ""))
				tablebody += addParentRowNone()
			}

/* 
			if len(parents) > 0 {
				tablebody += fmt.Sprintf("<td rowspan='%d'>• %s</td>", rowspan+1, strings.ReplaceAll(name, ".oet", ""))

				for parent := range parents {
					parentbits := strings.Split(parents[parent], "~")
					parentcid := parentbits[1]
					parenttitle := strings.ReplaceAll(parent, ".oet", "")
					tablebody += addParentRow(parentcid, parenttitle)
				}
			} else {
				tablebody += fmt.Sprintf("<td rowspan='%d'>• %s</td>", 2, strings.ReplaceAll(name, ".oet", ""))
				tablebody += addParentRowNone()
			}
 */
			tablebody += "</tr>"
		}
	} else {
		tablebody += "<tr>"
		tablebody += fmt.Sprintf("<td>%s</td>", "[ none ]")
		tablebody += "</tr>"

	}
	/* 	// TODO:
	   	// ------------------------------------------ List of all Order Sets (not via a group, directly embedded)  --------------------------------------------------------
	   	tablebody += "<tr>"
	   	tablebody += fmt.Sprintf("<td data-fill-color='eaecec' data-a-h='center'  data-f-bold='true' style='background-color: rgba(234, 236, 236, 0.6);;'><b>%s</b></td>", "List of all Order Sets (not via a group, directly embedded)")
	   	for i := 0; i < columnnumber; i++ {
	   		tablebody += "<td data-fill-color='eaecec' style='background-color: rgba(234, 236, 236, 0.6);;'><b></b></td>"
	   	}
	   	tablebody += "</tr>"

	   	tablebody += "<tr>"
	   	tablebody += fmt.Sprintf("<td>%s</td>", "[ none ]")
	   	tablebody += "</tr>"
	   	tablebody += "</tbody>"

	*/
	// ---- build page

	tabledef = tableheader + tablebody
	overlaptemplate, _ := readlines2("html/wurreporttemplate.html")

	exportbutton := fmt.Sprintf(`button.addEventListener("click", e => {
		let table = document.querySelector("#my-table");
			TableToExcel.convert(table, 
				{
					name: "WUR - %s.xlsx",
					sheet: { name: "Sheet 1" }
				}
			)
	  	});`, assetdisplayname)

	var line string
	for i := range overlaptemplate {
		line = overlaptemplate[i]
		line = strings.Replace(line, "<cdata>%%TABLE%%</cdata>", tabledef, -1)
		line = strings.Replace(line, "<cdata>%%EXPORT%%</cdata>", exportbutton, -1)
		line = strings.Replace(line, "%%ASSETNAME%%", assetdisplayname, -1)		
		*report += line
	}

	return true

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
	//logMessage("Doing Notification Dispatch", "", "INFO")

	query = `SELECT lastnotification
		FROM public.state;`
	rows, err := db.Query(query)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			fmt.Println("pq error:", err.Code.Name())
			logMessage(fmt.Sprintf("Problems querying state : %s", err.Code.Name()), "", "ERROR")
		}
		return false
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&lastid,
		)
	}

	query = `SELECT id, "lead", message, asset, created, notifymgr, jirakey 
			from notificationqueue
			where id > $1
			order by id asc`

	rows, err = db.Query(query, lastid)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			fmt.Println("pq error:", err.Code.Name())
			logMessage(fmt.Sprintf("Problems querying notifications[%d] : %s", lastid, err.Code.Name()), "", "ERROR")
		}
		return false
	}
	defer rows.Close()

	for rows.Next() {
		thebody := ""
		thejirakey := ""
		theasset := ""
		notifymgr := false
		lead := ""

		var whencreated pq.NullTime

		err = rows.Scan(
			&theid,
			&lead,

			&thebody,
			&theasset,
			&whencreated,
			&notifymgr,
			&thejirakey,
		)

		if err != nil {
			logMessage("Problems scanning notificationqueue"+err.Error(), "", "ERROR")
		}

		toperson := lead + "@ahs.ca"

		m := gomail.NewMessage()
		m.SetHeader("From", "noreply@ahs.ca", "DAM")
		m.SetHeader("To", toperson)

		if notifymgr {

			results := strings.Split(sessionConfig.ManagersEmail, ",")
			for _, email := range results {
				logMessage("Notifying manager : "+email, "", "DEBUG")
				m.SetAddressHeader("Cc", email, email)
			}

		}

		m.SetHeader("Subject", sessionConfig.SubjectPrefix+"DAM: "+thejirakey)
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
				logMessage(fmt.Sprintf("Problems updating state [%d] : %s", theid, err.Code.Name()), "", "ERROR")
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
	_, err := db.Exec(sqlStatement, sessionConfig.SubjectPrefix+": "+message, time.Now(), "DAMInform v"+gBuild, ticket, logtype)

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
		tablebody += fmt.Sprintf("<td>%s</td>", created.Time.Format("2006-01-02 15:04:05"))
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
