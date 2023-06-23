package com.javarush.reactivetest;

import lombok.Data;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootTest
class ReactiveTestApplicationTests {

    @Test
    public void fluxJust() {
        Flux<String> mealFlux = Flux.just("БигМак", "Чизбургер", "Гамбургер", "Картошка", "Кола");

        mealFlux.subscribe(
                f -> System.out.println("Порция фастфуда: " + f));

        StepVerifier.create(mealFlux)
                .expectNext("БигМак")
                .expectNext("Чизбургер")
                .expectNext("Гамбургер")
                .expectNext("Картошка")
                .expectNext("Кола")
                .verifyComplete();
    }


    @Test
    public void fluxFromArray() {
        String[] meal = new String[]{
                "БигМак", "Чизбургер", "Гамбургер", "Картошка", "Кола"};
        Flux<String> mealFlux = Flux.fromArray(meal);

        mealFlux.subscribe(
                f -> System.out.println("Порция фастфуда: " + f));

        StepVerifier.create(mealFlux)
                .expectNext("БигМак")
                .expectNext("Чизбургер")
                .expectNext("Гамбургер")
                .expectNext("Картошка")
                .expectNext("Кола")
                .verifyComplete();
    }

    //from Collection

    @Test
    public void fluxFromIterable() {
        List<String> mealList = new ArrayList<>();
        mealList.add("БигМак");
        mealList.add("Чизбургер");
        mealList.add("Гамбургер");
        mealList.add("Картошка");
        mealList.add("Кола");

        Flux<String> mealFlux = Flux.fromIterable(mealList);

        StepVerifier.create(mealFlux)
                .expectNext("БигМак")
                .expectNext("Чизбургер")
                .expectNext("Гамбургер")
                .expectNext("Картошка")
                .expectNext("Кола")
                .verifyComplete();
    }

    @Test
    public void fluxFromStream() {
        Stream<String> mealStream =
                Stream.of("БигМак", "Чизбургер", "Гамбургер", "Картошка", "Кола");
        Flux<String> mealFlux = Flux.fromStream(mealStream);
        StepVerifier.create(mealFlux)
                .expectNext("БигМак")
                .expectNext("Чизбургер")
                .expectNext("Гамбургер")
                .expectNext("Картошка")
                .expectNext("Кола")
                .verifyComplete();
    }

    //Generate

    @Test
    public void fluxRange() {
        Flux<Integer> intervalFlux = Flux.range(1, 5);

        StepVerifier.create(intervalFlux)
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .expectNext(4)
                .expectNext(5)
                .verifyComplete();
    }

    @Test
    public void fluxInterval() {
        Flux<Long> intervalFlux = Flux.interval(Duration.ofSeconds(1))
                .take(5);
        intervalFlux.subscribe(f -> System.out.println("The next number: " + f));

        StepVerifier.create(intervalFlux)
                .expectNext(0L)
                .expectNext(1L)
                .expectNext(2L)
                .expectNext(3L)
                .expectNext(4L)
                .verifyComplete();
    }

    //Combine
    @Test
    public void mergeFluxes() {
        Flux<String> characterFlux = Flux.just("Brad Pitt", "Tom Cruise", "Pierce Brosnan")
                .delayElements(Duration.ofMillis(500));

        Flux<String> carFlux = Flux
                .just("Lamborghini", "Toyota", "Aston Martin")
                .delaySubscription(Duration.ofMillis(250))
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergedFlux = characterFlux.mergeWith(carFlux);
        mergedFlux.subscribe(f -> System.out.println("The next value: " + f));
        StepVerifier.create(mergedFlux)
                .expectNext("Brad Pitt")
                .expectNext("Lamborghini")
                .expectNext("Tom Cruise")
                .expectNext("Toyota")
                .expectNext("Pierce Brosnan")
                .expectNext("Aston Martin")
                .verifyComplete();
    }

    @Test
    public void zipFluxes() {

        Flux<String> characterFlux = Flux.just("Brad Pitt", "Tom Cruise", "Pierce Brosnan");
        Flux<String> carFlux = Flux.just("Lamborghini", "Toyota", "Aston Martin");

        Flux<Tuple2<String, String>> zippedFlux = Flux.zip(characterFlux, carFlux);


        zippedFlux.subscribe(f -> System.out.println("The next zipped value: " + f));

        StepVerifier.create(zippedFlux)
                .expectNextMatches(p ->
                p.getT1().equals("Brad Pitt") &&
                        p.getT2().equals("Lamborghini"))
                .expectNextMatches(p ->
                p.getT1().equals("Tom Cruise") &&
                        p.getT2().equals("Toyota"))
                .expectNextMatches(p ->
                p.getT1().equals("Pierce Brosnan") &&
                        p.getT2().equals("Aston Martin"))
                .verifyComplete();
    }

    @Test
    public void zipFluxesToObject() {
        Flux<String> characterFlux = Flux.just("Brad Pitt", "Tom Cruise", "Pierce Brosnan");
        Flux<String> carFlux = Flux.just("Lamborghini", "Toyota", "Aston Martin");

        Flux<String> zippedFlux = Flux.zip(characterFlux, carFlux, (c, f) -> c + " drives " + f);

        StepVerifier.create(zippedFlux)
                .expectNext("Brad Pitt drives Lamborghini")
                .expectNext("Tom Cruise drives Toyota")
                .expectNext("Pierce Brosnan drives Aston Martin")
                .verifyComplete();
    }

    //firstFlux
    @Test
    public void firstWithSignalFlux() {
        Flux<String> slowFlux = Flux.just("Trabant", "Lada", "Ford T")
                .delaySubscription(Duration.ofMillis(100));
        Flux<String> fastFlux = Flux.just("Lamborgini", "Ferrari", "Bugatti");

        Flux<String> firstFlux = Flux.firstWithSignal(slowFlux, fastFlux);

        StepVerifier.create(firstFlux)
                .expectNext("Lamborgini")
                .expectNext("Ferrari")
                .expectNext("Bugatti")
                .verifyComplete();
    }


    //filtering
    @Test
    public void skipAFew() {
        Flux<String> characterFlux = Flux.just("Mr. Bean", "James Bond", "Ace Ventura", "Austin Powers", "Biff Tannen")
                .skip(3);
        StepVerifier.create(characterFlux)
                .expectNext("Austin Powers", "Biff Tannen")
                .verifyComplete();
    }


    @Test
    public void skipAFewSeconds() {
        Flux<String> characterFlux = Flux.just(
                        "Mr. Bean", "James Bond", "Ace Ventura", "Austin Powers", "Biff Tannen")
                .delayElements(Duration.ofSeconds(1))
                .skip(Duration.ofSeconds(4));
        StepVerifier.create(characterFlux)
                .expectNext("Austin Powers", "Biff Tannen")
                .verifyComplete();
    }

    @Test
    public void take() {
        Flux<String> characterFlux = Flux.just("Mr. Bean", "James Bond", "Ace Ventura", "Austin Powers", "Biff Tannen")
                .take(3);
        StepVerifier.create(characterFlux)
                .expectNext("Mr. Bean", "James Bond", "Ace Ventura")
                .verifyComplete();
    }

    @Test
    public void takeForAwhile() {
        Flux<String> characterFlux = Flux.just("Mr. Bean", "James Bond", "Ace Ventura", "Austin Powers", "Biff Tannen")
                .delayElements(Duration.ofSeconds(1))
                .take(Duration.ofMillis(3500));
        StepVerifier.create(characterFlux)
                .expectNext("Mr. Bean", "James Bond", "Ace Ventura")
                .verifyComplete();
    }


    @Test
    public void filter() {
        Flux<String> singersFlux = Flux.just("Michael Jackson", "Mariah Carey", "Bjork", "Billy Joel", "Prince")
                .filter(np -> !np.contains(" "));

        singersFlux.subscribe(f -> System.out.println("The next value: " + f));

        StepVerifier.create(singersFlux)
                .expectNext("Bjork")
                .expectNext("Prince")
                .verifyComplete();
    }


    @Test
    public void distinct() {
        Flux<String> singerFlux = Flux.just("Michael Jackson", "Bjork", "Michael Jackson", "Prince", "Bjork")
                .distinct();
        StepVerifier.create(singerFlux)
                .expectNext("Michael Jackson", "Bjork", "Prince")
                .verifyComplete();
    }

    //view
    @Test
    public void map() {
        Flux<Player> playerFlux = Flux
                .just("Michael Jordan", "Scottie Pippen", "Steve Kerr").map(n -> {
                    String[] split = n.split("\\s");
                    return new Player(split[0], split[1]);
                });
        StepVerifier.create(playerFlux)
                .expectNext(new Player("Michael", "Jordan"))
                .expectNext(new Player("Scottie", "Pippen"))
                .expectNext(new Player("Steve", "Kerr"))
                .verifyComplete();
    }

    @Data
    private static class Player {
        private final String firstName;
        private final String lastName;
    }


    @Test
    public void flatMap() {
        Flux<Player> playerFlux = Flux
                .just("Michael Jordan", "Scottie Pippen", "Steve Kerr")
                .flatMap(n -> Mono.just(n)
                        .map(p -> {
                            String[] split = p.split("\\s");
                            return new Player(split[0], split[1]);
                        }).subscribeOn(Schedulers.parallel())
                );
        List<Player> playerList = Arrays.asList(
                new Player("Michael", "Jordan"),
                new Player("Scottie", "Pippen"),
                new Player("Steve", "Kerr"));

        StepVerifier.create(playerFlux)
                .expectNextMatches(p -> playerList.contains(p))
                .expectNextMatches(p -> playerList.contains(p))
                .expectNextMatches(p -> playerList.contains(p))
                .verifyComplete();
    }

    //bufferization

    @Test
    public void buffer() {
        Flux<String> singerFlux = Flux.just("Michael Jackson", "Mariah Carey", "Bjork", "Billy Joel", "Prince");
        Flux<List<String>> bufferedFlux = singerFlux.buffer(3);

        StepVerifier
                .create(bufferedFlux)
                .expectNext(Arrays.asList("Michael Jackson", "Mariah Carey", "Bjork"))
                .expectNext(Arrays.asList("Billy Joel", "Prince"))
                .verifyComplete();
    }

    @Test
    public void collectList() {
        Flux<String> singerFlux = Flux.just(
                "Michael Jackson", "Mariah Carey", "Bjork", "Billy Joel", "Prince");
        Mono<List<String>> fruitListMono = singerFlux.collectList();

        StepVerifier
                .create(fruitListMono)
                .expectNext(Arrays.asList("Michael Jackson", "Mariah Carey", "Bjork", "Billy Joel", "Prince"))
                .verifyComplete();
    }

    //!
    @Test
    public void collectMap() {
        Flux<String> singerFlux = Flux.just("Michael Jackson", "Mariah Carey", "Bjork", "Billy Joel", "Prince");
        Mono<Map<Character, String>> animalMapMono = singerFlux.collectMap(a -> a.charAt(0));
        StepVerifier.create(animalMapMono)
                .expectNextMatches(map -> {
                    return
                            map.size() == 3 && map.get('M').equals("Mariah Carey") &&
                            map.get('B').equals("Billy Joel") &&
                            map.get('P').equals("Prince");
                }).verifyComplete();
    }


    @Test
    public void all() {
        Flux<String> characterFlux = Flux.just("Mr. Bean", "James Bond", "Ace Ventura", "Batman", "Biff Tannen");

        Mono<Boolean> hasAMono = characterFlux.all(a -> a.contains("a"));
        StepVerifier.create(hasAMono)
                .expectNext(true)
                .verifyComplete();

        Mono<Boolean> hasSMono = characterFlux.all(a -> a.contains("s"));
        StepVerifier.create(hasSMono)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    public void any() {
        Flux<String> characterFlux = Flux.just("Mr. Bean", "James Bond", "Ace Ventura", "Batman", "Biff Tannen");

        Mono<Boolean> hasTMono = characterFlux.any(a -> a.contains("t"));
        StepVerifier.create(hasTMono)
                .expectNext(true) .verifyComplete();

        Mono<Boolean> hasXMono = characterFlux.any(a -> a.contains("x"));
        StepVerifier.create(hasXMono)
                .expectNext(false)
                .verifyComplete();
    }


}
