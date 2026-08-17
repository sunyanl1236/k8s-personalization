package lab.personalization.generator.domain;

// Minimal set for Phase 4's CEP pattern (view, then cart, then no checkout
// within a window). "Left without checking out" is the absence of a
// CHECKOUT event within the pattern's own bound, not a fourth value here.
public enum ActionType {
    VIEW,
    ADD_TO_CART,
    CHECKOUT
}
