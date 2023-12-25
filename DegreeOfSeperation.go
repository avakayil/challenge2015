//done by Ajay Vakayil (25/12/2023)
//avakayil22@gmail.com
//8848895580

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Person struct {
	Name   string `json:"name"`
	Movies []struct {
		URL string `json:"url"`
	} `json:"movies"`
}
type Movie struct {
	Cast []struct {
		URL string `json:"url"`
	} `json:"cast"`
	Crew []struct {
		URL string `json:"url"`
	} `json:"crew"`
}
type Connection struct {
	Person *Person
	Movie  *Movie
	Role   string
	Degree int
	Path   []string
}

var limiter = rate.NewLimiter(1000, 5000)

func getPerson(url string, wg *sync.WaitGroup, personChan chan<- *Person) error {
	defer wg.Done()
	var person Person
	err := fetchAndUnmarshal(url, &person)
	if err != nil {
		return err
	}
	personChan <- &person
	return nil
}

func getMovie(url string, wg *sync.WaitGroup, movieChan chan<- *Movie) error {
	defer wg.Done()
	var movie Movie
	err := fetchAndUnmarshal(url, &movie)
	if err != nil {
		return err
	}
	movieChan <- &movie
	return nil
}

func main() {
	start := time.Now()
	var wgPerson, wgMovie, wgClose sync.WaitGroup
	personChan := make(chan *Person, 20000)
	movieChan := make(chan *Movie, 1000)
	wgPerson.Add(2)
	file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	logger := log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	logger.Println("Program to find the degree of separation of 2 artist started.....")
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter the name of the first artist ")
	artist1, _ := reader.ReadString('\n')
	artist1 = strings.TrimSuffix(artist1, "\r\n")
	fmt.Print("Enter the name of the second artist ")
	artist2, _ := reader.ReadString('\n')
	artist2 = strings.TrimSuffix(artist2, "\r\n")
	fmt.Println("finding the degree of separation...")
	fmt.Println("processing...")
	if err := getPerson(convert(artist1), &wgPerson, personChan); err != nil {
		logger.Fatalln("Error fetching the first artist : ", err)
		return
	}
	if err := getPerson(convert(artist2), &wgPerson, personChan); err != nil {
		logger.Fatalln("Error fetching the second artist : ", err)
		return
	}
	wgPerson.Wait()
	startPerson := <-personChan
	endPerson := <-personChan
	forwardQueue := []*Connection{{Person: startPerson, Degree: 0, Path: []string{startPerson.Name}}}
	backwardQueue := []*Connection{{Person: endPerson, Degree: 0, Path: []string{endPerson.Name}}}
	forwardVisited := make(map[string]*Connection)
	backwardVisited := make(map[string]*Connection)
	for len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		connection := forwardQueue[0]
		forwardQueue = forwardQueue[1:]
		person := connection.Person
		if backwardConnection, ok := backwardVisited[person.Name]; ok {
			fmt.Println("Degree of connection:", connection.Degree+backwardConnection.Degree-1)
			fmt.Println("Path of connection: 0 ", append(connection.Path, backwardConnection.Path...))
			length := len(connection.Path)
			for i := 0; i < length-1; i++ {
				fmt.Println(connection.Path[i], " and ", connection.Path[i+1], " worked in the movie  ", findTheMovieRelation(connection.Path[i], connection.Path[i+1]))
			}
			length = len(backwardConnection.Path)
			for i := 0; i < length-1; i++ {
				fmt.Println(backwardConnection.Path[i], " and ", backwardConnection.Path[i+1], " worked in the movie  ", findTheMovieRelation(backwardConnection.Path[i], backwardConnection.Path[i+1]))
			}
			elapsed := time.Since(start)
			fmt.Printf("The code executed in %s\n", elapsed)
			return
		}
		for _, movieURL := range person.Movies {
			wgMovie.Add(1)
			wgClose.Add(1)
			go func(url string) {
				defer wgClose.Done()
				if err := getMovie("http://data.moviebuff.com/"+url, &wgMovie, movieChan); err != nil {
					logger.Println("Error fetching movie : ", err)
				}
			}(movieURL.URL)
		}
		wgMovie.Wait()
		wgClose.Wait()
		close(movieChan)
		for movie := range movieChan {
			for _, actorURL := range movie.Cast {
				_, ok := forwardVisited[actorURL.URL]
				if actorURL.URL != "" && !ok {
					wgPerson.Add(1)
					wgClose.Add(1)
					go func(url string) {
						defer wgClose.Done()
						if err := getPerson("http://data.moviebuff.com/"+url, &wgPerson, personChan); err != nil {
							logger.Println("Error fetching person : ", err)
						}
					}(actorURL.URL)
				}
			}
		}
		wgPerson.Wait()
		wgClose.Wait()
		close(personChan)
		for person := range personChan {
			if _, ok := forwardVisited[person.Name]; !ok {
				newPath := append(connection.Path, person.Name)
				forwardQueue = append(forwardQueue, &Connection{Person: person, Degree: connection.Degree + 1, Path: newPath})
				forwardVisited[person.Name] = &Connection{Person: person, Degree: connection.Degree + 1, Path: newPath}
			}
		}
		personChan = make(chan *Person, 20000)
		movieChan = make(chan *Movie, 1000)
		connection = backwardQueue[0]
		backwardQueue = backwardQueue[1:]
		person = connection.Person
		if forwardConnection, ok := forwardVisited[person.Name]; ok {
			fmt.Println("Degree of connection:", connection.Degree+forwardConnection.Degree-1)
			fmt.Println("Path of connection: ", append(forwardConnection.Path, connection.Path...))
			length := len(forwardConnection.Path)
			for i := 0; i < length-1; i++ {
				fmt.Println(forwardConnection.Path[i], " and ", forwardConnection.Path[i+1], " worked in the movie  ", findTheMovieRelation(forwardConnection.Path[i], forwardConnection.Path[i+1]))
			}
			length = len(connection.Path)
			for i := 0; i < length-1; i++ {
				fmt.Println(connection.Path[i], " ", connection.Path[i+1], " worked in the movie   ", findTheMovieRelation(connection.Path[i], connection.Path[i+1]))
			}
			elapsed := time.Since(start)
			fmt.Printf("The code executed in %s\n", elapsed)
			return
		}
		for _, movieURL := range person.Movies {
			wgMovie.Add(1)
			wgClose.Add(1)
			go func(url string) {
				defer wgClose.Done()
				if err := getMovie("http://data.moviebuff.com/"+url, &wgMovie, movieChan); err != nil {
					logger.Println("Error fetching movie : ", err)
				}
			}(movieURL.URL)
		}
		wgMovie.Wait()
		wgClose.Wait()
		close(movieChan)
		for movie := range movieChan {
			for _, actorURL := range movie.Cast {
				_, ok := backwardVisited[actorURL.URL]
				if actorURL.URL != "" && !ok {
					wgPerson.Add(1)
					wgClose.Add(1)
					go func(url string) {
						defer wgClose.Done()
						if err := getPerson("http://data.moviebuff.com/"+url, &wgPerson, personChan); err != nil {
							logger.Println("Error fetching person : ", err)
						}
					}(actorURL.URL)
				}
			}
		}
		wgPerson.Wait()
		wgClose.Wait()
		close(personChan)
		for person := range personChan {
			if _, ok := backwardVisited[person.Name]; !ok {
				newPath := append(connection.Path, person.Name)
				backwardQueue = append(backwardQueue, &Connection{Person: person, Degree: connection.Degree + 1, Path: newPath})
				backwardVisited[person.Name] = &Connection{Person: person, Degree: connection.Degree + 1, Path: newPath}
			}
		}
		personChan = make(chan *Person, 20000)
		movieChan = make(chan *Movie, 1000)
	}
	fmt.Println("No path found between", startPerson.Name, "and", endPerson.Name)
	elapsed := time.Since(start)
	fmt.Printf("The code executed in %s\n", elapsed)
}
func fetchAndUnmarshal(url string, v interface{}) error {
	limiter.Wait(context.Background())
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Go-http-client/1.1")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Microsecond)
	return json.Unmarshal(body, v)
}

func convert(input string) string {
	lowercase := strings.ToLower(input)
	return "http://data.moviebuff.com/" + strings.ReplaceAll(lowercase, " ", "-")
}

func findTheMovieRelation(casturl1, casturl2 string) string {
	casturl1 = convert(casturl1)
	casturl2 = convert(casturl2)
	var person1 Person
	err := fetchAndUnmarshal(casturl1, &person1)
	if err != nil {
		fmt.Println("Error fetching person:", err, casturl1)
		return ""
	}
	var person2 Person
	err = fetchAndUnmarshal(casturl2, &person2)
	if err != nil {
		fmt.Println("Error fetching person:", err, casturl1)
		return ""
	}
	person1Map := make(map[string]bool)
	for _, movie := range person1.Movies {
		person1Map[movie.URL] = true
	}
	for _, movie := range person2.Movies {
		if person1Map[movie.URL] == true {
			return movie.URL
		}
	}
	return ""
}
