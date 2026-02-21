package com.helix.benchmark.datagen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class ReferenceRegistry {
    private final List<String> advisorIds = new CopyOnWriteArrayList<>();
    private final List<String> investorIds = new CopyOnWriteArrayList<>();
    private final List<String> advisoryContextIds;
    private final List<Long> partyRoleIds;
    private final List<Long> finInstIds;
    private final List<String> fundTickers;
    private final List<String> hierarchyPathValues;

    public ReferenceRegistry(int advisoryContextPoolSize, int partyRoleIdPoolSize, int finInstIdPoolSize) {
        this.advisoryContextIds = generateAdvisoryContextIds(advisoryContextPoolSize);
        this.partyRoleIds = generatePartyRoleIds(partyRoleIdPoolSize);
        this.finInstIds = generateFinInstIds(finInstIdPoolSize);
        this.fundTickers = generateFundTickers(200);
        this.hierarchyPathValues = generateHierarchyPathValues(500);
    }

    private List<String> generateAdvisoryContextIds(int count) {
        List<String> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(String.valueOf(1_000_000_000_000L + i));
        }
        return Collections.unmodifiableList(ids);
    }

    private List<Long> generatePartyRoleIds(int count) {
        List<Long> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(35_000_000L + i);
        }
        return Collections.unmodifiableList(ids);
    }

    private List<Long> generateFinInstIds(int count) {
        List<Long> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(100L + i);
        }
        return Collections.unmodifiableList(ids);
    }

    private List<String> generateFundTickers(int count) {
        List<String> tickers = new ArrayList<>(count);
        String[] bases = {"VTI", "SPY", "QQQ", "IWM", "EFA", "AGG", "BND", "GLD", "TLT", "LQD",
                "VXUS", "VEA", "VWO", "IEMG", "HYG", "MUB", "VCIT", "VCSH", "VTIP", "BSV"};
        for (String base : bases) {
            tickers.add(base);
        }
        for (int i = tickers.size(); i < count; i++) {
            StringBuilder sb = new StringBuilder();
            int len = ThreadLocalRandom.current().nextInt(2, 6);
            for (int j = 0; j < len; j++) {
                sb.append((char) ('A' + ThreadLocalRandom.current().nextInt(26)));
            }
            tickers.add(sb.toString());
        }
        return Collections.unmodifiableList(tickers);
    }

    private List<String> generateHierarchyPathValues(int count) {
        List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (i % 3 == 0) {
                values.add(String.valueOf(100 + i)); // Firm-like
            } else if (i % 3 == 1) {
                values.add((100 + i) + "-Region"); // Region-like
            } else {
                values.add(String.valueOf(800_000_000L + i)); // IPPersonTeam-like
            }
        }
        return Collections.unmodifiableList(values);
    }

    public void registerAdvisorId(String advisorId) {
        advisorIds.add(advisorId);
    }

    public void registerInvestorId(String investorId) {
        investorIds.add(investorId);
    }

    public List<String> getAdvisorIds() {
        return Collections.unmodifiableList(advisorIds);
    }

    public List<String> getInvestorIds() {
        return Collections.unmodifiableList(investorIds);
    }

    public List<String> getAdvisoryContextIds() {
        return advisoryContextIds;
    }

    public List<Long> getPartyRoleIds() {
        return partyRoleIds;
    }

    public List<Long> getFinInstIds() {
        return finInstIds;
    }

    public String randomAdvisorId() {
        return randomFrom(advisorIds);
    }

    public String randomInvestorId() {
        return randomFrom(investorIds);
    }

    public String randomAdvisoryContextId() {
        return randomFrom(advisoryContextIds);
    }

    public Long randomPartyRoleId() {
        return randomFrom(partyRoleIds);
    }

    public Long randomFinInstId() {
        return randomFrom(finInstIds);
    }

    public String randomFundTicker() {
        return randomFrom(fundTickers);
    }

    public String randomHierarchyPathValue() {
        return randomFrom(hierarchyPathValues);
    }

    public List<String> randomAdvisorIds(int count) {
        return randomSubset(advisorIds, count);
    }

    public List<String> randomInvestorIds(int count) {
        return randomSubset(investorIds, count);
    }

    private <T> T randomFrom(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private <T> List<T> randomSubset(List<T> list, int count) {
        int actualCount = Math.min(count, list.size());
        List<T> shuffled = new ArrayList<>(list);
        Collections.shuffle(shuffled, ThreadLocalRandom.current());
        return shuffled.subList(0, actualCount);
    }
}
