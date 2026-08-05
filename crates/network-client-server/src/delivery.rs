use wattswarm_network_transport_core::DeliveryClass;

pub trait DeliveryScheduler {
    fn next_class(
        &mut self,
        interactive_available: bool,
        bulk_available: bool,
    ) -> Option<DeliveryClass>;
}

#[derive(Debug, Clone)]
pub struct WeightedDeliveryScheduler {
    interactive_burst_pages: usize,
    interactive_since_bulk: usize,
}

impl WeightedDeliveryScheduler {
    pub fn new(interactive_burst_pages: usize) -> Self {
        Self {
            interactive_burst_pages: interactive_burst_pages.max(1),
            interactive_since_bulk: 0,
        }
    }
}

impl DeliveryScheduler for WeightedDeliveryScheduler {
    fn next_class(
        &mut self,
        interactive_available: bool,
        bulk_available: bool,
    ) -> Option<DeliveryClass> {
        if bulk_available
            && (!interactive_available
                || self.interactive_since_bulk >= self.interactive_burst_pages)
        {
            self.interactive_since_bulk = 0;
            return Some(DeliveryClass::Bulk);
        }
        if interactive_available {
            self.interactive_since_bulk = self.interactive_since_bulk.saturating_add(1);
            return Some(DeliveryClass::Interactive);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactive_is_preferred_but_bulk_cannot_starve() {
        let mut scheduler = WeightedDeliveryScheduler::new(2);
        let selected: Vec<_> = (0..6)
            .map(|_| scheduler.next_class(true, true).unwrap())
            .collect();
        assert_eq!(
            selected,
            vec![
                DeliveryClass::Interactive,
                DeliveryClass::Interactive,
                DeliveryClass::Bulk,
                DeliveryClass::Interactive,
                DeliveryClass::Interactive,
                DeliveryClass::Bulk,
            ]
        );
    }
}
