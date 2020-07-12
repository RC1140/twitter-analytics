package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"flag"

	"github.com/coreos/pkg/flagutil"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	bolt "go.etcd.io/bbolt"
)

func itob(v uint16) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func loadExistingCounts(db *bolt.DB, activeBucket string) map[string]uint16 {
	userCounts := make(map[string]uint16)
	db.Update(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		//TODO : Clean up bucket name generation
		_, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s.%s", "dailycount", activeBucket)))
		return err
	})
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(fmt.Sprintf("%s.%s", "dailycount", activeBucket)))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			c := binary.BigEndian.Uint16(v)
			userCounts[string(k)] = c
		}

		return nil
	})
	return userCounts
}

func updateCount(db *bolt.DB, count uint16, user, activeBucket string) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fmt.Sprintf("%s.%s", "dailycount", activeBucket)))

		err := b.Put([]byte(user), itob(count))
		//TODO : Replace format strings with propper debug logs
		// fmt.Printf("Saving data for %s %d\n", user, count)
		if err != nil {
			return fmt.Errorf("Error updating key: %s", err)
		}
		return err
	})

}

func putLastIndexedTweet(db *bolt.DB, tweetID int64) {
	db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("lastindexedtweet"))
		if err != nil {
			return err
		}
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(tweetID))

		err = b.Put([]byte("id"), id)
		fmt.Println("Index saved")
		if err != nil {
			return fmt.Errorf("Error updating index: %s", err)
		}
		return err
	})

}

func getLastIndexedTweeID(db *bolt.DB) int64 {
	var lastIndexedTweet int64
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastindexedtweet"))

		if b != nil {
			rawID := b.Get([]byte("id"))
			c := binary.BigEndian.Uint64(rawID)
			lastIndexedTweet = int64(c)
		}
		return nil
	})
	return lastIndexedTweet
}

func getDailyUserCountBuckets(db *bolt.DB) []string {
	var buckets []string
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			bn := string(k)
			if strings.HasPrefix(bn, "dailycount") {
				buckets = append(buckets, bn)
			}
		}

		return nil
	})
	return buckets
}

func dumpBucketToCSV(db *bolt.DB, bucket string) {
	var userStats [][]string
	userStats = append(userStats, []string{"bucket_date", "screen_name", "daily_tweets"})
	bucketDate := strings.Split(bucket, ".")[1]
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			c := binary.BigEndian.Uint16(v)
			userStats = append(userStats, []string{bucketDate, string(k), fmt.Sprintf("%d", c)})
		}

		return nil
	})

	w := csv.NewWriter(os.Stdout)
	w.WriteAll(userStats) // calls Flush internally

	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

//dbExport is used to automate extracting the stats
func dbExport(db *bolt.DB) {
	bcks := getDailyUserCountBuckets(db)

	for _, bck := range bcks {
		dumpBucketToCSV(db, bck)
	}

}

func main() {
	db, err := bolt.Open("analytics.db", 0666, nil)
	if err != nil {
		fmt.Println(err)
	}

	defer db.Close()

	flags := flag.NewFlagSet("user-auth", flag.ExitOnError)

	consumerKey := flags.String("consumer-key", "", "Twitter Consumer Key")
	consumerSecret := flags.String("consumer-secret", "", "Twitter Consumer Secret")
	accessToken := flags.String("access-token", "", "Twitter Access Token")
	accessSecret := flags.String("access-secret", "", "Twitter Access Secret")
	dumpCSV := flags.Bool("csv", false, "Set to true to dump all buckets to csv")

	flags.Parse(os.Args[1:])
	flagutil.SetFlagsFromEnv(flags, "TWITTER")

	if *dumpCSV {
		dbExport(db)
		return
	}

	config := oauth1.NewConfig(*consumerKey, *consumerSecret)
	token := oauth1.NewToken(*accessToken, *accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	client := twitter.NewClient(httpClient)

	var lastIndexedTweet int64 = 0

	for {
		activeBucket := time.Now().Format("01/02/06")
		//We technically dont need to load the data for long running proceses , this just ensures a bit more consistency if the app ever crashes
		m := loadExistingCounts(db, activeBucket)
		lastIndexedTweet = getLastIndexedTweeID(db)

		fmt.Printf("Last indexed tweet %d \n", lastIndexedTweet)

		tweets, _, err := client.Timelines.HomeTimeline(&twitter.HomeTimelineParams{
			Count:   200,
			SinceID: lastIndexedTweet,
		})
		if err != nil {
			fmt.Println(err)
		}

		if len(tweets) > 0 {
			fmt.Printf("Found %d tweets since last run", len(tweets))
			if tweets != nil {
				for _, x := range tweets {
					m[x.User.ScreenName]++
					if x.ID > lastIndexedTweet {
						lastIndexedTweet = x.ID
					}
				}
			}

			for user, count := range m {
				updateCount(db, count, user, activeBucket)
			}

			putLastIndexedTweet(db, lastIndexedTweet)
		} else {
			fmt.Println("No New tweets found exiting")
		}

		fmt.Println("Sleeping for 5 mins")
		time.Sleep(5 * time.Minute)
	}

}
