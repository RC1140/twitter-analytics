package main

import (
	"encoding/binary"
	"fmt"
	"os"
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
		fmt.Printf("Saving data for %s %d\n", user, count)
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

func main() {
	db, err := bolt.Open("analytics.db", 0666, nil)
	if err != nil {
		fmt.Println(err)
	}

	activeBucket := time.Now().Format("01/02/06")

	defer db.Close()

	flags := flag.NewFlagSet("user-auth", flag.ExitOnError)

	consumerKey := flags.String("consumer-key", "", "Twitter Consumer Key")
	consumerSecret := flags.String("consumer-secret", "", "Twitter Consumer Secret")
	accessToken := flags.String("access-token", "", "Twitter Access Token")
	accessSecret := flags.String("access-secret", "", "Twitter Access Secret")

	flags.Parse(os.Args[1:])
	flagutil.SetFlagsFromEnv(flags, "TWITTER")

	config := oauth1.NewConfig(*consumerKey, *consumerSecret)
	token := oauth1.NewToken(*accessToken, *accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	m := loadExistingCounts(db, activeBucket)

	client := twitter.NewClient(httpClient)
	var lastIndexedTweet int64 = 0

	lastIndexedTweet = getLastIndexedTweeID(db)

	fmt.Printf("Last indexed tweet %d \n", lastIndexedTweet)

	tweets, _, err := client.Timelines.HomeTimeline(&twitter.HomeTimelineParams{
		Count:   200,
		SinceID: lastIndexedTweet,
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Found %d tweets since last run", len(tweets))

	if tweets != nil {
		for _, x := range tweets {
			// fmt.Println(fmt.Sprintf("-> %s", x.User.ScreenName))
			// fmt.Println(fmt.Sprintf("** %s", x.Text))
			m[x.User.ScreenName]++
			if x.ID > lastIndexedTweet {
				lastIndexedTweet = x.ID
			}

		}
	}

	putLastIndexedTweet(db, lastIndexedTweet)
	for user, count := range m {
		updateCount(db, count, user, activeBucket)
	}
}
