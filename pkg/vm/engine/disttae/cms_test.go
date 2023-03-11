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
	g1 := make(map[string][]string)
	g2 := make(map[string][]string)
	c1 := 0
	c2 := 0
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

			start1 := strings.Index(str, "group:<") + len("group:<")
			end1 := strings.Index(str, ">")
			groupid := str[start1:end1]

			if tim, ok := m1[tblid]; ok {
				m1[tblid] = tim + 1
			} else {
				m1[tblid] = 1
			}

			if s, ok := g1[tblid]; ok {
				c1++
				g1[tblid] = append(s, groupid)
				fmt.Printf("table %s receive more than 1 mo_table insert\n", tblid)
			} else {
				g1[tblid] = []string{groupid}
			}
		}

		// mo columns
		if strings.Contains(str, "insert mo_column") {
			start := strings.Index(str, "id:[") + len("id:[")
			end := strings.Index(str, "]")
			tblid := str[start:end]

			start1 := strings.Index(str, "group:<") + len("group:<")
			end1 := strings.Index(str, ">")
			groupid := str[start1:end1]

			if tim, ok := m2[tblid]; ok {
				m2[tblid] = tim + 1
			} else {
				m2[tblid] = 1
			}

			if s, ok := g2[tblid]; ok {
				g2[tblid] = append(s, groupid)
				fmt.Printf("table %s receive more than 1 mo_column insert\n", tblid)
			} else {
				c2++
				g2[tblid] = []string{groupid}
			}
		}
	}
	fmt.Printf("m1 is %d, c1 is %d\n m2 is %d, c2 is %d\n",
		len(m1), c1, len(m2), c2)

	fmt.Printf("\n\n")

	for table, group := range g1 {
		if group2, ok := g2[table]; !ok {
			fmt.Printf("table %s get insert table but without insert column\n", table)
		} else {
			for _, gg1 := range group {
				find := false
				for _, gg2 := range group2 {
					if gg1 == gg2 {
						find = true
						break
					}
				}
				if !find {
					fmt.Printf("table %s group %s insert table without columns\n", table, gg1)
				}
			}
		}
	}
}
