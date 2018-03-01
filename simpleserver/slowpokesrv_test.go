package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"testing"
)

func TestNil(t *testing.T) {
	var v []byte
	if v == nil {
		log.Println("dont panic")
	}
	if len(v) == 0 {
		log.Println("dont panic")
	}

}

//run server before testing
func TestCatPut(t *testing.T) {
	//init
	post := "http://localhost:5000/slowpoke/post/%06d"
	category := "http://localhost:5000/slowpoke/categories/%s:%06d"
	client := &http.Client{}

	type Post struct {
		Id       int
		Content  string
		Category string
	}

	// Put 100 urls

	for i := 0; i < 40; i++ {

		url := fmt.Sprintf(post, i)
		post := &Post{Id: i, Content: "Content:" + strconv.Itoa(i), Category: "Category:" + strconv.Itoa(i/10)}
		b, _ := json.Marshal(post)
		request, err := http.NewRequest("PUT", url, bytes.NewReader(b))
		if err != nil {
			t.Error(err)
		}
		response, err := client.Do(request)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		status := response.StatusCode
		fmt.Println(status, url)

		//Put categories
		urlcateg := fmt.Sprintf(category, strconv.Itoa(i/10), i)
		requestCat, err := http.NewRequest("PUT", urlcateg, nil)
		if err != nil {
			t.Error(err)
		}
		responseCat, err := client.Do(requestCat)
		if err != nil {
			log.Fatal(err)
		}
		defer responseCat.Body.Close()
		fmt.Println(responseCat.StatusCode, urlcateg)
	}

	//Get last 10 keys
	urlkeys := "http://localhost:5000/slowpoke/post?order=desc&cnt=10&offset=0"
	request, err := http.NewRequest("POST", urlkeys, nil)
	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	var posts []string
	json.Unmarshal(b, &posts)
	fmt.Println(posts)

	//get vals
	urlval := "http://localhost:5000/slowpoke/post/%s"
	for _, postid := range posts {
		response, err := http.Get(fmt.Sprintf(urlval, postid))
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		b, _ := ioutil.ReadAll(response.Body)
		var post Post
		json.Unmarshal(b, &post)
		fmt.Println(post)
	}

	//get postid by categories
	urlcat := "http://localhost:5000/slowpoke/categories/2:*?order=desc&cnt=5&offset=0"
	reqcats, err := http.NewRequest("POST", urlcat, nil)
	respcats, err := client.Do(reqcats)
	if err != nil {
		log.Fatal(err)
	}
	defer respcats.Body.Close()
	bcats, err := ioutil.ReadAll(respcats.Body)
	if err != nil {
		log.Fatal(err)
	}
	var postsByCat []string
	json.Unmarshal(bcats, &postsByCat)
	fmt.Println(postsByCat)

}

/*
Output:

2018/03/01 10:37:53 dont panic
200 http://localhost:5000/slowpoke/post/000000
200 http://localhost:5000/slowpoke/categories/0:000000
200 http://localhost:5000/slowpoke/post/000001
200 http://localhost:5000/slowpoke/categories/0:000001
200 http://localhost:5000/slowpoke/post/000002
200 http://localhost:5000/slowpoke/categories/0:000002
200 http://localhost:5000/slowpoke/post/000003
200 http://localhost:5000/slowpoke/categories/0:000003
200 http://localhost:5000/slowpoke/post/000004
200 http://localhost:5000/slowpoke/categories/0:000004
200 http://localhost:5000/slowpoke/post/000005
200 http://localhost:5000/slowpoke/categories/0:000005
200 http://localhost:5000/slowpoke/post/000006
200 http://localhost:5000/slowpoke/categories/0:000006
200 http://localhost:5000/slowpoke/post/000007
200 http://localhost:5000/slowpoke/categories/0:000007
200 http://localhost:5000/slowpoke/post/000008
200 http://localhost:5000/slowpoke/categories/0:000008
200 http://localhost:5000/slowpoke/post/000009
200 http://localhost:5000/slowpoke/categories/0:000009
200 http://localhost:5000/slowpoke/post/000010
200 http://localhost:5000/slowpoke/categories/1:000010
200 http://localhost:5000/slowpoke/post/000011
200 http://localhost:5000/slowpoke/categories/1:000011
200 http://localhost:5000/slowpoke/post/000012
200 http://localhost:5000/slowpoke/categories/1:000012
200 http://localhost:5000/slowpoke/post/000013
200 http://localhost:5000/slowpoke/categories/1:000013
200 http://localhost:5000/slowpoke/post/000014
200 http://localhost:5000/slowpoke/categories/1:000014
200 http://localhost:5000/slowpoke/post/000015
200 http://localhost:5000/slowpoke/categories/1:000015
200 http://localhost:5000/slowpoke/post/000016
200 http://localhost:5000/slowpoke/categories/1:000016
200 http://localhost:5000/slowpoke/post/000017
200 http://localhost:5000/slowpoke/categories/1:000017
200 http://localhost:5000/slowpoke/post/000018
200 http://localhost:5000/slowpoke/categories/1:000018
200 http://localhost:5000/slowpoke/post/000019
200 http://localhost:5000/slowpoke/categories/1:000019
200 http://localhost:5000/slowpoke/post/000020
200 http://localhost:5000/slowpoke/categories/2:000020
200 http://localhost:5000/slowpoke/post/000021
200 http://localhost:5000/slowpoke/categories/2:000021
200 http://localhost:5000/slowpoke/post/000022
200 http://localhost:5000/slowpoke/categories/2:000022
200 http://localhost:5000/slowpoke/post/000023
200 http://localhost:5000/slowpoke/categories/2:000023
200 http://localhost:5000/slowpoke/post/000024
200 http://localhost:5000/slowpoke/categories/2:000024
200 http://localhost:5000/slowpoke/post/000025
200 http://localhost:5000/slowpoke/categories/2:000025
200 http://localhost:5000/slowpoke/post/000026
200 http://localhost:5000/slowpoke/categories/2:000026
200 http://localhost:5000/slowpoke/post/000027
200 http://localhost:5000/slowpoke/categories/2:000027
200 http://localhost:5000/slowpoke/post/000028
200 http://localhost:5000/slowpoke/categories/2:000028
200 http://localhost:5000/slowpoke/post/000029
200 http://localhost:5000/slowpoke/categories/2:000029
200 http://localhost:5000/slowpoke/post/000030
200 http://localhost:5000/slowpoke/categories/3:000030
200 http://localhost:5000/slowpoke/post/000031
200 http://localhost:5000/slowpoke/categories/3:000031
200 http://localhost:5000/slowpoke/post/000032
200 http://localhost:5000/slowpoke/categories/3:000032
200 http://localhost:5000/slowpoke/post/000033
200 http://localhost:5000/slowpoke/categories/3:000033
200 http://localhost:5000/slowpoke/post/000034
200 http://localhost:5000/slowpoke/categories/3:000034
200 http://localhost:5000/slowpoke/post/000035
200 http://localhost:5000/slowpoke/categories/3:000035
200 http://localhost:5000/slowpoke/post/000036
200 http://localhost:5000/slowpoke/categories/3:000036
200 http://localhost:5000/slowpoke/post/000037
200 http://localhost:5000/slowpoke/categories/3:000037
200 http://localhost:5000/slowpoke/post/000038
200 http://localhost:5000/slowpoke/categories/3:000038
200 http://localhost:5000/slowpoke/post/000039
200 http://localhost:5000/slowpoke/categories/3:000039
[000039 000038 000037 000036 000035 000034 000033 000032 000031 000030]
{39 Content:39 Category:3}
{38 Content:38 Category:3}
{37 Content:37 Category:3}
{36 Content:36 Category:3}
{35 Content:35 Category:3}
{34 Content:34 Category:3}
{33 Content:33 Category:3}
{32 Content:32 Category:3}
{31 Content:31 Category:3}
{30 Content:30 Category:3}
[2:000029 2:000028 2:000027 2:000026 2:000025]
PASS
ok      github.com/recoilme/slowpoke/simpleserver       0.107s
*/
