package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
)

// City represents a city bleve index
type City struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ASCIIName  string `json:"asciiname"`
	CountryID  string `json:"country_id"`
	Timezone   string `json:"timezone"`
	Population int64  `json:"population"`
	Latitude   string `json:"latitude"`
	Longitude  string `json:"longitude"`
}

// SearchResultIcon this is the nested object to return country icon
type SearchResultIcon struct {
	Path string `json:"path"`
}

// SearchResult The actual json to return to Alfred
type SearchResult struct {
	UID          string           `json:"uid"`
	Title        string           `json:"title"`
	Subtitle     string           `json:"subtitle"`
	Arg          string           `json:"arg"`
	Autocomplete string           `json:"autocomplete"`
	Icon         SearchResultIcon `json:"icon"`
}

// Airport Object to part airport file
type Airport struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Code        string  `json:"code"`
	CountryCode string  `json:"country_code"`
	Lat         float64 `json:"lat"`
	Long        float64 `json:"long"`
	Type        string  `json:"type"`
	Distance    float64
}

// OpenIndex Return the bleve city index content
func OpenIndex(databasePath string) bleve.Index {
	index, err := bleve.Open(databasePath)
	if err != nil {
		panic(err)
	}

	return index
}

// HaversineDistance calculates the distance between 2 lat longs
func HaversineDistance(la1, lg1, la2, lg2 float64) float64 {
	lat1 := la1 * math.Pi / 180
	long1 := lg1 * math.Pi / 180
	lat2 := la2 * math.Pi / 180
	long2 := lg2 * math.Pi / 180

	diffLat := lat2 - lat1
	diffLong := long2 - long1

	a := math.Pow(math.Sin(diffLat/2), 2) + math.Cos(lat1)*math.Cos(lat2)*math.Pow(math.Sin(diffLong/2), 2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return c * 6371
}

// GetPhoneCodesByCountry returns map of country code -> phone number prefix
func GetPhoneCodesByCountry(phoneFile string) map[string]string {
	cdata, err := ioutil.ReadFile(phoneFile)
	if err != nil {
		panic(err)
	}

	var data map[string]string
	jsonErr := json.Unmarshal(cdata, &data)
	if jsonErr != nil {
		panic(jsonErr)
	}

	return data
}

// GetCurrencyByCountry returns map of country code -> currency
func GetCurrencyByCountry(currencyFile string) map[string]string {
	cdata, err := ioutil.ReadFile(currencyFile)
	if err != nil {
		panic(err)
	}

	var data map[string]string
	jsonErr := json.Unmarshal(cdata, &data)
	if jsonErr != nil {
		panic(jsonErr)
	}

	return data
}

// GetAirportsByCountry returns map of country code -> airports
func GetAirportsByCountry(airportFile string) map[string][]Airport {
	airports := make(map[string][]Airport)

	cdata, err := ioutil.ReadFile(airportFile)
	if err != nil {
		panic(err)
	}

	var data []Airport
	jsonErr := json.Unmarshal(cdata, &data)
	if jsonErr != nil {
		panic(jsonErr)
	}

	for _, airport := range data {
		airports[airport.CountryCode] = append(airports[airport.CountryCode], airport)
	}

	return airports
}

// GetCountryMap returns map of country code -> country name
func GetCountryMap(countryFile string) map[string]string {
	countries := make(map[string]string)
	cdata, err := ioutil.ReadFile(countryFile)
	if err != nil {
		panic(err)
	}

	var data interface{}
	jsonErr := json.Unmarshal(cdata, &data)
	if jsonErr != nil {
		panic(jsonErr)
	}

	for _, country := range data.([]interface{}) {
		countries[country.(map[string]interface{})["Code"].(string)] = country.(map[string]interface{})["Name"].(string)
	}

	return countries
}

// CreateIndex does the initial indexing of city data
func CreateIndex(databasePath string, citiFile string) {
	cityData, err := ioutil.ReadFile(citiFile)
	if err != nil {
		panic(err)
	}

	cities := []City{}
	if err := json.Unmarshal(cityData, &cities); err != nil {
		panic(err)
	}

	mapping := bleve.NewIndexMapping()

	cityMapping := bleve.NewDocumentMapping()

	asciiNameMapping := bleve.NewTextFieldMapping()
	cityMapping.AddFieldMappingsAt("asciiname", asciiNameMapping)

	cityIDMapping := bleve.NewTextFieldMapping()
	cityIDMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("id", cityIDMapping)

	nameMapping := bleve.NewTextFieldMapping()
	cityMapping.AddFieldMappingsAt("name", nameMapping)

	countryIDMapping := bleve.NewTextFieldMapping()
	countryIDMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("country_id", countryIDMapping)

	timezoneMapping := bleve.NewTextFieldMapping()
	timezoneMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("timezone", timezoneMapping)

	populationMapping := bleve.NewNumericFieldMapping()
	populationMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("population", populationMapping)

	latitudeMapping := bleve.NewTextFieldMapping()
	latitudeMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("latitude", latitudeMapping)

	longitudeMapping := bleve.NewTextFieldMapping()
	longitudeMapping.IncludeInAll = false
	cityMapping.AddFieldMappingsAt("longitude", longitudeMapping)

	mapping.AddDocumentMapping("city", cityMapping)

	index, err := bleve.New(databasePath, mapping)
	if err != nil {
		panic(err)
	}

	for _, city := range cities {
		index.Index(city.ID, city)
	}

}

// FindClosestAirport returns list of airports from a city's lat long
func FindClosestAirport(lat, long float64, airports []Airport) []Airport {
	matches := []Airport{}

	for _, airport := range airports {
		distance := HaversineDistance(lat, long, airport.Lat, airport.Long)
		airport.Distance = distance
		matches = append(matches, airport)
	}

	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].Distance < matches[j].Distance
	})

	filter := matches[:int64(math.Min(float64(len(matches)), 5))]
	sort.SliceStable(filter, func(i, j int) bool {
		return filter[i].Type < filter[j].Type
	})

	return filter[:int64(math.Min(float64(len(filter)), 3))]
}

func searchCity(searchTerm string, index bleve.Index) (*City, error) {
	query := bleve.NewMatchQuery(searchTerm)
	search := bleve.NewSearchRequest(query)
	search.SortBy([]string{"-_score", "-population"})
	searchResults, err := index.Search(search)
	if err != nil {
		panic(err)
	}

	if len(searchResults.Hits) == 0 {
		return &City{}, errors.New("No results found")
	}

	results := []City{}

	for i := range searchResults.Hits {
		raw, _ := index.Document(searchResults.Hits[i].ID)
		result := City{
			string(raw.Fields[0].Value()),
			string(raw.Fields[1].Value()),
			string(raw.Fields[2].Value()),
			string(raw.Fields[3].Value()),
			string(raw.Fields[4].Value()),
			int64(binary.BigEndian.Uint64(raw.Fields[5].Value())),
			string(raw.Fields[6].Value()),
			string(raw.Fields[7].Value()),
		}
		results = append(results, result)
	}

	return &results[0], nil
}

func performSearch(cities []string, cityIndex bleve.Index) []City {
	runner := make(chan City)

	var wg sync.WaitGroup
	wg.Add(len(cities))

	for _, item := range cities {
		go func(item string, idx bleve.Index) {
			defer wg.Done()
			res, _ := searchCity(item, cityIndex)
			runner <- *res
		}(item, cityIndex)
	}

	results := []City{}
	for i := 0; i < len(cities); i++ {
		result := <-runner
		if result.ID != "" {
			results = append(results, result)
		}
	}

	wg.Wait()
	close(runner)

	return results
}

func formatResults(results []City,
	countryIndex map[string]string,
	airportIndex map[string][]Airport,
	phoneIndex map[string]string,
	currencyIndex map[string]string,
) string {

	output := make([]SearchResult, len(results))
	for i, city := range results {
		if city.ID == "" {
			continue
		}

		loc, _ := time.LoadLocation(city.Timezone)
		now := time.Now().In(loc)
		zone, _ := now.Zone()
		icon := strings.ToLower(strings.Replace(countryIndex[city.CountryID], " ", "_", -1))
		icon = fmt.Sprintf("flags/%s.png", icon)

		if _, err := os.Stat(icon); os.IsNotExist(err) {
			icon = "flags/_no_flag.png"
		}

		lat, _ := strconv.ParseFloat(city.Latitude, 64)
		long, _ := strconv.ParseFloat(city.Longitude, 64)
		airports := FindClosestAirport(lat, long, airportIndex[city.CountryID])

		airportString := make([]string, len(airports))
		for i := 0; i < len(airports); i++ {
			airportString[i] = airports[i].Code
		}

		subtitle := []string{
			now.Format("Monday, January 2"),
			countryIndex[city.CountryID],
			fmt.Sprintf("+%s", phoneIndex[city.CountryID]),
			currencyIndex[city.CountryID],
			strings.Join(airportString, ","),
		}

		output[i] = SearchResult{
			UID:          city.ID,
			Title:        fmt.Sprintf("%s — %s %s", city.Name, now.Format("3:04 PM"), zone),
			Subtitle:     strings.Join(subtitle, " | "),
			Arg:          city.Name,
			Autocomplete: city.Name,
			Icon:         SearchResultIcon{icon},
		}
	}

	str, _ := json.Marshal(output)

	return fmt.Sprintf(`{"items":%s}`, string(str))
}

func main() {
	indexFile := "cities.bleve"
	cityFile := "datasets/cities.json"
	countryFile := "datasets/countries.json"
	airportFile := "datasets/airports.json"
	phoneFile := "datasets/phone.json"
	currencyFile := "datasets/currency.json"
	input := strings.Join(os.Args[1:], " ")

	if input == "" {
		panic("Must specify query")
	}

	if _, err := os.Stat(indexFile); os.IsNotExist(err) {
		CreateIndex(indexFile, cityFile)
	}

	cityIndex := OpenIndex(indexFile)
	countryIndex := GetCountryMap(countryFile)
	airportIndex := GetAirportsByCountry(airportFile)
	phoneIndex := GetPhoneCodesByCountry(phoneFile)
	currencyIndex := GetCurrencyByCountry(currencyFile)

	searchTerm := strings.Split(input, ",")
	results := performSearch(searchTerm, cityIndex)
	payload := formatResults(results, countryIndex, airportIndex, phoneIndex, currencyIndex)

	fmt.Println(payload)

}
