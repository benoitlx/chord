#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Id<const M: u32>(u64);

impl<const M: u32> Id<M> {
    const MASK: u64 = {
        assert!(M >= 1 && M <= 64, "M should be between 1 and 64");
        u64::MAX >> (64 - M)
    };

    pub const fn new(raw: u64) -> Self {
        Self(raw & Self::MASK)
    }

    pub const fn finger_target(self, i: u32) -> Self {
        assert!(i < M, "i should be less than M");
        Self::new(self.0.wrapping_add(1u64 << i))
    }

    pub const fn in_open_closed(self, a: Self, b: Self) -> bool {
        if a.0 > b.0 {
            return self.0 > a.0 || self.0 <= b.0;
        } else if a.0 == b.0 {
            return true;
        }

        self.0 > a.0 && self.0 <= b.0
    }

    pub const fn in_open(self, a: Self, b: Self) -> bool {
        if a.0 > b.0 {
            return self.0 > a.0 || self.0 < b.0;
        } else if a.0 == b.0 {
            return self.0 != a.0;
        }

        self.0 > a.0 && self.0 < b.0
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::Id;

    type Id6 = Id<6>;

    fn id(raw: u64) -> Id6 {
        Id6::new(raw)
    }

    #[test]
    fn new_ramene_la_valeur_dans_l_anneau() {
        assert_eq!(id(63).value(), 63);
        assert_eq!(id(64).value(), 0);
        assert_eq!(id(70).value(), 6);
        assert_eq!(Id::<1>::new(3).value(), 1);
        assert_eq!(Id::<64>::new(u64::MAX).value(), u64::MAX);
    }

    #[test]
    fn cibles_des_doigts_de_n8() {
        let cibles: Vec<u64> = (0..6).map(|i| id(8).finger_target(i).value()).collect();
        assert_eq!(cibles, [9, 10, 12, 16, 24, 40]);
    }

    #[test]
    fn cible_de_doigt_qui_reboucle() {
        assert_eq!(id(60).finger_target(3).value(), 4);
        assert_eq!(Id::<64>::new(u64::MAX).finger_target(0).value(), 0);
        assert_eq!(Id::<64>::new(1).finger_target(63).value(), (1 << 63) + 1);
    }

    #[test]
    #[should_panic(expected = "i should be less than M")]
    fn indice_de_doigt_trop_grand() {
        id(8).finger_target(6);
    }

    #[test]
    fn open_closed_sans_rebouclage() {
        assert!(id(14).in_open_closed(id(8), id(14)));
        assert!(id(10).in_open_closed(id(8), id(14)));
        assert!(!id(8).in_open_closed(id(8), id(14)));
        assert!(!id(15).in_open_closed(id(8), id(14)));
    }

    #[test]
    fn open_closed_avec_rebouclage() {
        assert!(id(61).in_open_closed(id(60), id(5)));
        assert!(id(0).in_open_closed(id(60), id(5)));
        assert!(id(5).in_open_closed(id(60), id(5)));
        assert!(!id(60).in_open_closed(id(60), id(5)));
        assert!(!id(6).in_open_closed(id(60), id(5)));
    }

    #[test]
    fn open_closed_couvre_tout_l_anneau_si_bornes_egales() {
        assert!(id(8).in_open_closed(id(8), id(8)));
        assert!(id(9).in_open_closed(id(8), id(8)));
    }

    #[test]
    fn open_exclut_les_deux_bornes() {
        assert!(id(10).in_open(id(8), id(14)));
        assert!(!id(8).in_open(id(8), id(14)));
        assert!(!id(14).in_open(id(8), id(14)));
        assert!(id(0).in_open(id(60), id(5)));
        assert!(!id(5).in_open(id(60), id(5)));
    }

    #[test]
    fn open_exclut_seulement_a_si_bornes_egales() {
        assert!(!id(8).in_open(id(8), id(8)));
        assert!(id(9).in_open(id(8), id(8)));
    }
}
