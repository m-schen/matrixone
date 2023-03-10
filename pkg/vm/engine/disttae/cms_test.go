package disttae

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestCMS(t *testing.T) {
	f, _ := os.Open("/Users/chenmingsong/Downloads/e2e_docker-compose/cms3.log")
	defer f.Close()
	reader := bufio.NewReader(f)
	m1 := make(map[string]int)
	m2 := make(map[string]int)
	for {
		str, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		str = strings.Replace(str, "\r\n", "", -1)
		// mo tables
		if strings.Contains(str, "insert mo_table") {
			start := strings.Index(str, "id:[") + len("id:[")
			end := strings.Index(str, "]")
			tblid := str[start:end]
			if tim, ok := m1[tblid]; ok {
				m1[tblid] = tim + 1
			} else {
				m1[tblid] = 1
			}
		}
		// mo columns
		if strings.Contains(str, "insert mo_column") {
			start := strings.Index(str, "id:[") + len("id:[")
			end := strings.Index(str, "]")
			tblid := str[start:end]
			if tim, ok := m2[tblid]; ok {
				m2[tblid] = tim + 1
			} else {
				m2[tblid] = 1
			}
		}
	}

	c1 := 0
	for table, time1 := range m1 {
		if time1 > 1 {
			c1++
			println(table)
		}
	}
	println("whole table is ", len(m1), "more than 1 is ", c1)

	println("xxxxxxx")
	c2 := 0
	for table, time2 := range m2 {
		if time2 > 1 {
			c1++
			println(table)
		}
	}
	println("whole column is ", len(m2), "more than 1 is ", c2)

	println("xxxxxxx")
	for table, time1 := range m1 {
		if time1 > 1 {
			println(fmt.Sprintf("get too much table of %s,", table))
		}
		if time2, ok := m2[table]; !ok {
			println(fmt.Sprintf("lost column of %s", table))
		} else {
			if time1 != time2 {
				if time2 > 1 {
					println(fmt.Sprintf("get too much column of %s,", table))
				} else {
					println(fmt.Sprintf("get column of %s, but not equal", table))
				}
			}
		}
	}
	for col := range m2 {
		if _, ok := m1[col]; !ok {
			println(fmt.Sprintf("lost table of %s", col))
		}
	}
}
