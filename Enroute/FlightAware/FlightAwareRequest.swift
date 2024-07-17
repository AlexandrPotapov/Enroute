//
//  FlightAwareRequest.swift
//  Enroute
//
//  Created by Alexander on 15.08.2022.
//

import Foundation
import Combine

// очень простой последовательный сборщик данных FlightAware по расписанию
// используя FlightAware REST API
// достаточно для поддержки наших демонстрационных потребностей
// имеет некоторое простое кэширование, чтобы сделать запуск/остановку в демо постоянно
// чтобы не перегружать его запросами FlightAware
// (также, запросы FlightAware не бесплатны!)
// также есть простой "режим симуляции"
// так что он будет "работать", когда нет действительных учетных данных FlightAware

// для того, чтобы это действительно получало данные от FlightAware
// вам нужен аккаунт FlightAware и ключ API
// (забор данных не бесплатный, подробности см. на сайте flightaware.com/api)
// поместите имя учетной записи и ключ API в Info.plist
// под ключом "FlightAware Credentials"
// пример учетных данных: "joepilot:2ab78c93fccc11f999999111030304"
// если этот ключ не существует, автоматически включится режим симуляции

class FlightAwareRequest<Fetched> where Fetched: Codable, Fetched: Hashable
{

    private(set) var results = CurrentValueSubject<Set<Fetched>, Never>([])

    let batchSize = 15
    var offset: Int = 0
    lazy var howMany: Int = batchSize
    private(set) var fetchInterval: TimeInterval = 0

    // MARK: - Subclassers Overrides
    
    var cacheKey: String? { return nil }
    var query: String { "" }
    func decode(_ json: Data) -> Set<Fetched> { Set<Fetched>() }
    func filter(_ results: Set<Fetched>) -> Set<Fetched> { results }
    var fetchTimer: Timer?

    // MARK: - Private Data
    
    private var captureSimulationData = false
    
    private var urlRequest: URLRequest? { Self.authorizedURLRequest(query: query) }
    private var fetchCancellable: AnyCancellable?
    private var fetchSequenceCount: Int = 0
    
    private var cacheData: Data? { cacheKey != nil ? UserDefaults.standard.data(forKey: cacheKey!) : nil }
    private var cacheTimestampKey: String { (cacheKey ?? "")+".timestamp" }
    private var cacheAge: TimeInterval? {
        let since1970 = UserDefaults.standard.double(forKey: cacheTimestampKey)
        if since1970 > 0 {
            return Date.currentFlightTime.timeIntervalSince1970 - since1970
        } else {
            return nil
        }
    }

    // MARK: - Fetching
    
    func fetch(andRepeatEvery interval: TimeInterval, useCache: Bool? = nil) {
        fetchInterval = interval
        if useCache != nil {
            fetch(useCache: useCache!)
        } else {
            fetch()
        }
    }
    
    func stopFetching() {
        fetchCancellable?.cancel()
        fetchTimer?.invalidate()
        fetchInterval = 0
        fetchSequenceCount = 0
    }
    
    func fetch(useCache: Bool = true) {
        if !useCache || !fetchFromCache() {
            if let urlRequest = self.urlRequest {
                print("fetching \(urlRequest)")
                if offset == 0 { fetchSequenceCount = 0 }
                fetchCancellable = URLSession.shared.dataTaskPublisher(for: urlRequest)
                    .map { [weak self] data, response in
                        if self?.captureSimulationData ?? false {
                            flightSimulationData[self?.query ?? ""] = data.utf8
                        }
                        return self?.decode(data) ?? []
                    }
                    .replaceError(with: [])
                    .receive(on: DispatchQueue.main)
                    .sink { [weak self] results in self?.handleResults(results) }
            } else {
                if let json = flightSimulationData[query]?.data(using: .utf8) {
                    print("simulating \(query)")
                    handleResults(decode(json), isCacheable: false)
                }
            }
        }
    }
    

    private func handleResults(_ newResults: Set<Fetched>, age: TimeInterval = 0, isCacheable: Bool = true) {
        let existingCount = results.value.count
        let newValue = fetchSequenceCount > 0 ? results.value.union(newResults) : newResults.union(results.value)
        let added = newValue.count - existingCount
        results.value = filter(newValue)
        let sequencing = age == 0 && added == batchSize && results.value.count < howMany && fetchSequenceCount < (howMany-(batchSize-1))/batchSize
        let interval = sequencing ? 1 : (age > 0 && age < fetchInterval) ? fetchInterval - age : fetchInterval
        if isCacheable, age == 0, !sequencing {
            cache(newValue)
        }
        if interval > 0 {
            if sequencing {
                fetchSequenceCount += 1
            } else {
                offset = 0
                fetchSequenceCount = 0
            }
            fetchTimer = Timer.scheduledTimer(withTimeInterval: interval, repeats: false, block: { [weak self] timer in
                if (self?.fetchInterval ?? 0) > 0 || (self?.fetchSequenceCount ?? 0) > 0 {
                    self?.fetch()
                }
            })
        }
    }
    
    // MARK: - Cacheing

    private func fetchFromCache() -> Bool { // returns whether we were able to
        if fetchSequenceCount == 0, let key = cacheKey, let age = cacheAge {
            if age > 0, (fetchInterval == 0) || (age < fetchInterval) || urlRequest == nil, let data = cacheData {
                if let cachedResults = try? JSONDecoder().decode(Set<Fetched>.self, from: data) {
                    print("using \(Int(age))s old cache \(key)")
                    handleResults(cachedResults, age: age)
                    return true
                } else {
                    print("couldn't decode information from \(Int(age))s old cache \(cacheKey!)")
                }
            }
        }
        return false
    }
    
    private func cache(_ results: Set<Fetched>) {
        if let key = self.cacheKey, let data = try? JSONEncoder().encode(results) {
            print("caching \(key) at \(DateFormatter.short.string(from: Date.currentFlightTime))")
            UserDefaults.standard.set(Date.currentFlightTime.timeIntervalSince1970, forKey: self.cacheTimestampKey)
            UserDefaults.standard.set(data, forKey: key)
        }
    }
    
    // MARK: - Utility
        
    static func authorizedURLRequest(query: String, credentials: String? = Bundle.main.object(forInfoDictionaryKey: "FlightAware Credentials") as? String) -> URLRequest? {
        let flightAware = "https://flightxml.flightaware.com/json/FlightXML2/"
        if let url = URL(string: flightAware + query), let credentials = (credentials?.isEmpty ?? true) ? nil : credentials?.base64 {
            var request = URLRequest(url: url)
            request.setValue("Basic \(credentials)", forHTTPHeaderField: "Authorization")
            return request
        }
        return nil
    }
}

// MARK: - Extensions

extension String {
    mutating func addFlightAwareArgument(_ name: String, _ value: Int? = nil, `default` defaultValue: Int = 0) {
        if value != nil, value != defaultValue {
            addFlightAwareArgument(name, "\(value!)")
        }
    }
    mutating func addFlightAwareArgument(_ name: String, _ value: Date?) {
        if value != nil {
            addFlightAwareArgument(name, "\(Int(value!.timeIntervalSince1970))")
        }
    }
    
    mutating func addFlightAwareArgument(_ name: String, _ value: String?) {
        if value != nil {
            self += (hasSuffix("?") ? "" : "&") + name + "=" + value!
        }
    }
}

// MARK: - Simulation Support


extension Date {
    private static let launch = Date()
        
    static var currentFlightTime: Date {
        let credentials = Bundle.main.object(forInfoDictionaryKey: "FlightAware Credentials") as? String
        if credentials == nil || credentials!.isEmpty, !flightSimulationData.isEmpty, let simulationDate = flightSimulationDate {
            return simulationDate.addingTimeInterval(Date().timeIntervalSince(launch))
        } else {
            return Date()
        }
    }
}
