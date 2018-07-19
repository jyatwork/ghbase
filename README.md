# ghbase
hbase operator, use "github.com/tsuna/gohbase"


#for example:

package main
import(
	"github.com/tsuna/gohbase"
	"fmt"
	"ghbase"
)
func main(){
	zkquorum := "10.2.5.XXX"
	zkRoot := "/hbase"
	option1 := gohbase.ZookeeperRoot(zkRoot)
	option2 := gohbase.EffectiveUser("greejsj")
	ghbase.InitHbase(zkquorum,option1,option2)
	str,_ := ghbase.Gets("table","rowkey","colfamily","col")
	fmt.Println(str)
}